import asyncio
import logging
import os
from datetime import datetime, timedelta
from aiogram.fsm.state import StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram import types
import traceback
from asyncpg import Record
from aiogram import Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.types import Chat
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State
from aiogram.filters import Command
from aiogram.types import TelegramObject
from aiogram.dispatcher.flags import get_flag
from aiogram.client.default import DefaultBotProperties
import calendar
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import json

load_dotenv()

# ===================== CONFIG =====================

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))


def parse_dict(value: str) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        print("ENV PARSE ERROR:", e, value)
        return {}

ALLOWED_ASSIGNEES = parse_dict(os.getenv("ALLOWED_ASSIGNEES"))
ALLOWED_USERS = parse_dict(os.getenv("ALLOWED_USERS"))

CABINET_GROUP_IDS = [
    int(x.strip())
    for x in os.getenv("CABINET_GROUP_IDS", "").split(",")
    if x.strip()
]

ALLOWED_TASK_GROUPS = [
    int(x.strip())
    for x in os.getenv("ALLOWED_TASK_GROUPS", "").split(",")
    if x.strip()
]

ROOT_GROUP_ID = int(os.getenv("ROOT_GROUP_ID", 0))

router = Router()

BOT_PASSWORD = os.getenv("BOT_PASSWORD")

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

class AddTaskFSM(StatesGroup):
    waiting_text = State()
    waiting_date = State()
    waiting_time = State()

class EditTaskFSM(StatesGroup):
    waiting_text = State()
    waiting_date = State()
    waiting_time = State()

class EditDateFSM(StatesGroup):
    waiting_date = State()
    waiting_time = State()

class CabinetStates(StatesGroup):
    choosing_employee = State()
    entering_room = State()
    

# ===================== LOGGING =====================

logging.basicConfig(level=logging.INFO)

# ===================== BOT INIT =====================

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler()

# ===================== DATABASE =====================

db: asyncpg.Pool = None


async def init_db():
    global db
    db = await asyncpg.create_pool(dsn=POSTGRES_DSN)

class AuthState(StatesGroup):
    waiting_password = State()

# ===================== UTILS =====================

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить задачу", callback_data="add_task")],
        [InlineKeyboardButton(text="📅 Задачи на сегодня", callback_data="today_tasks")],
        [InlineKeyboardButton(text="📆 Задачи на завтра", callback_data="tomorrow_tasks")],
        [InlineKeyboardButton(text="🗂 Все задачи", callback_data="all_tasks")],
        [InlineKeyboardButton(text="🧑‍💼 Мои задачи", callback_data="my_tasks")],
        [InlineKeyboardButton(text="📌 Назначенные мне", callback_data="assigned_to_me")],
        [InlineKeyboardButton(text="📋 Кабинеты", callback_data="cabinet")]
    ])


# ===================== COMMANDS =====================

@router.message(Command("start", "menu"))
async def start_cmd(message: Message, state: FSMContext):
    user = await db.fetchrow(
        "SELECT authorized FROM bot_users WHERE user_id=$1",
        message.from_user.id
    )

    if not user or not user["authorized"]:
        await message.answer("🔐 Введите пароль доступа:")
        await state.set_state(AuthState.waiting_password)
        return

    await message.answer(
        "👋 <b>Добро пожаловать в менеджер задач</b>\n\nВыбери действие:",
        reply_markup=main_menu()
    )

@router.message(AuthState.waiting_password)
async def check_password(message: Message, state: FSMContext):
    if message.text != BOT_PASSWORD:
        await message.answer("❌ Неверный пароль. Попробуй ещё раз:")
        return

    await db.execute(
        """
        INSERT INTO bot_users (user_id, authorized)
        VALUES ($1, TRUE)
        ON CONFLICT (user_id)
        DO UPDATE SET authorized=TRUE
        """,
        message.from_user.id
    )

    await state.clear()

    await message.answer(
        "✅ Доступ разрешён!\n\n👋 <b>Добро пожаловать в менеджер задач</b>\n\nВыбери действие:",
        reply_markup=main_menu()
    )

async def is_authorized(user_id: int) -> bool:
    row = await db.fetchrow(
        "SELECT authorized FROM bot_users WHERE user_id=$1",
        user_id
    )
    return bool(row and row["authorized"])

@router.message(Command("arh"))
async def archive_cmd(message: Message):
    rows = await db.fetch(
        """
        SELECT text, task_datetime
        FROM tasks
        WHERE assigned_user_id = $1 AND completed = TRUE
        ORDER BY task_datetime DESC
        """,
        message.from_user.id
    )

    if not rows:
        await message.answer("Архив пуст.")
        return

    for row in rows:
        await message.answer(
            f"✅ <b>{row['text']}</b>\n"
            f"📅 {row['task_datetime'].strftime('%d.%m.%Y %H:%M')}"
        )

def calendar_kb(year: int, month: int):
    kb = []

    kb.append([
        InlineKeyboardButton(text="◀️", callback_data=f"cal_prev_{year}_{month}"),
        InlineKeyboardButton(text=f"{calendar.month_name[month]} {year}", callback_data="ignore"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal_next_{year}_{month}")
    ])

    week_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    kb.append([InlineKeyboardButton(text=d, callback_data="ignore") for d in week_days])

    for week in calendar.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
            else:
                row.append(
                    InlineKeyboardButton(
                        text=str(day),
                        callback_data=f"cal_day_{year}_{month}_{day}"
                    )
                )
        kb.append(row)

    return InlineKeyboardMarkup(inline_keyboard=kb)

# ===================== ЛС: ДОБАВИТЬ ЗАДАЧУ =====================
@router.callback_query(F.data == "add_task")
async def add_task(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddTaskFSM.waiting_text)
    await callback.message.answer("✍️ Напиши текст задачи:")
    await callback.answer()

@router.message(AddTaskFSM.waiting_text)
async def get_text(message: Message, state: FSMContext):
    await state.update_data(text=message.text)
    now = datetime.now()
    await state.set_state(AddTaskFSM.waiting_date)
    await message.answer(
        "📅 Выбери дату:",
        reply_markup=calendar_kb(now.year, now.month) 
    )

@router.callback_query(lambda c: c.data.startswith("cal_"))
async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    data = callback.data.split("_")
    if data[1] == "day":
        year, month, day = int(data[2]), int(data[3]), int(data[4])
        await state.update_data(date=f"{year}-{month:02d}-{day:02d}")
        await state.set_state(AddTaskFSM.waiting_time)
        await callback.message.answer("⏰ Введи время задачи в формате ЧЧ:ММ")
        await callback.answer()
    elif data[1] in ("prev", "next"):
        year, month = int(data[2]), int(data[3])
        if data[1] == "prev":
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        else:
            month += 1
            if month == 13:
                month = 1
                year += 1
        await callback.message.edit_reply_markup(reply_markup=calendar_kb(year, month))
        await callback.answer()

@router.message(AddTaskFSM.waiting_time)
async def get_time(message: Message, state: FSMContext):
    try:
        hour, minute = map(int, message.text.split(":"))
    except ValueError:
        await message.answer("❌ Неверный формат. Используй ЧЧ:ММ")
        return

    data = await state.get_data()
    dt = datetime.strptime(f"{data['date']} {hour:02d}:{minute:02d}", "%Y-%m-%d %H:%M")

    # создаём задачу в БД
    row = await db.fetchrow(
        """
        INSERT INTO tasks (user_id, text, task_datetime, created_at)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        message.from_user.id,
        data["text"],
        dt,
        datetime.now()
    )
    task_id = row["id"]

    # отправляем в основную группу
    group_msg = await bot.send_message(
        chat_id=GROUP_ID,
        text=f"📌 <b>Задача</b>\n{data['text']}\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}",
        parse_mode="HTML"
    )

    # сохраняем message_id
    await db.execute("UPDATE tasks SET group_msg_id=$1 WHERE id=$2", group_msg.message_id, task_id)

    await message.answer(f"✅ Задача создана и отправлена в основную группу:\n{data['text']}")
    await state.clear()


@router.callback_query(lambda c: c.data.startswith("cal_"))
async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    data = callback.data.split("_")


    # 1. Выбор конкретного дня

    if data[1] == "day":
        # безопасная распаковка
        if len(data) < 5:
            await callback.answer("Ошибка календаря!")
            return

        prefix, action, year, month, day = data
        year, month, day = int(year), int(month), int(day)

        # Сохраняем выбранную дату в state
        await state.update_data(date=f"{year}-{month}-{day}")

        # Проверяем текущее состояние FSM
        current_state = await state.get_state()
        if current_state == EditDateFSM.waiting_date.state:
            await state.set_state(EditDateFSM.waiting_time)
            await callback.message.answer("⏰ Введи новое время (ЧЧ:ММ)")
        elif current_state == AddTaskFSM.waiting_date.state:
            await state.set_state(AddTaskFSM.waiting_time)
            await callback.message.answer("⏰ Введи время (ЧЧ:ММ)")
        else:
            await callback.answer("Неверное состояние FSM!")


    # 2. Переход на предыдущий месяц

    elif data[1] == "prev":
        if len(data) < 4:
            await callback.answer("Ошибка календаря!")
            return

        prefix, action, year, month = data
        year, month = int(year), int(month) - 1
        if month == 0:
            month = 12
            year -= 1
        await callback.message.edit_reply_markup(reply_markup=calendar_kb(year, month))

    # 3. Переход на следующий месяц

    elif data[1] == "next":
        if len(data) < 4:
            await callback.answer("Ошибка календаря!")
            return

        prefix, action, year, month = data
        year, month = int(year), int(month) + 1
        if month == 13:
            month = 1
            year += 1
        await callback.message.edit_reply_markup(reply_markup=calendar_kb(year, month))


    # 4. Подтверждение выбора

    await callback.answer()


async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    
    await callback.answer()

@router.message(AddTaskFSM.waiting_time)
async def get_time(message: Message, state: FSMContext):
    try:
        hour, minute = map(int, message.text.split(":"))
    except ValueError:
        await message.answer("❌ Неверный формат. Используй ЧЧ:ММ")
        return

    data = await state.get_data()
    dt = datetime.strptime(
        f"{data['date']} {hour}:{minute}",
        "%Y-%m-%d %H:%M"
    )

    row = await db.fetchrow(
        """
        INSERT INTO tasks (user_id, text, task_datetime, created_at)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        message.from_user.id,
        data["text"],
        dt,                 # TIMESTAMP ✅
        datetime.now()      # TIMESTAMP ✅
    )

async def get_employees():
    async with db.acquire() as conn:
            return await conn.fetch("""
            SELECT id, full_name, room
            FROM employees
            ORDER BY full_name
        """)


    scheduler.add_job(
        bot.send_message,
        "date",
        run_date=dt,
        args=[GROUP_ID, f"🆕 <b>Задача:</b>\n\n{data['text']}"]
    )

    await message.answer(
        f"✅ <b>Задача создана на {dt.strftime('%d.%m.%Y %H:%M')}</b>"
    )

    row = await db.fetchrow(
    """
    INSERT INTO tasks (user_id, text, task_datetime, created_at, next_send_at)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id
    """,
    message.from_user.id,
    data["text"],
    dt,
    datetime.now(),
    datetime.now()  # первая отправка сразу через 3 часа
)

    await state.clear()
# ===================== CAB =====================

@router.callback_query(F.data == "cabinet")
async def open_cabinets(callback: CallbackQuery):
    employees = await db.fetch(
        "SELECT id, full_name, room, active FROM employees WHERE active=TRUE ORDER BY full_name"
    )

    kb = []
    for emp in employees:
        kb.append([
            InlineKeyboardButton(
                text=f"{emp['full_name']} — {emp['room'] or 'Не указан'}",
                callback_data=f"edit_room_{emp['id']}"
            ),
            InlineKeyboardButton(
                text="❌ Удалить",
                callback_data=f"delete_emp_{emp['id']}"
            )
        ])

    # Кнопки для отправки и добавления нового сотрудника
    kb.append([
        InlineKeyboardButton(
            text="➕ Добавить сотрудника",
            callback_data="add_employee"
        )
    ])
    kb.append([
        InlineKeyboardButton(
            text="Разослать в чаты",
            callback_data="send_cabinets_main"
        )
    ])

    await callback.message.answer(
        "📋 <b>Список кабинетов:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()

# -------------------- ДОБАВИТЬ СОТРУДНИКА --------------------
@router.callback_query(F.data == "add_employee")
async def add_employee(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CabinetStates.entering_room)
    await state.update_data(action="add")
    await callback.message.answer("Введите ФИО нового сотрудника:")
    await callback.answer()

@router.message(CabinetStates.entering_room)
async def save_employee_or_room(message: Message, state: FSMContext):
    data = await state.get_data()
    action = data.get("action")

    if action == "add":
        full_name = message.text.strip()
        await db.execute(
            "INSERT INTO employees (full_name, active) VALUES ($1, TRUE)",
            full_name
        )
        await message.answer(f"✅ Сотрудник добавлен: {full_name}")
        await state.clear()
    elif action == "edit":
        emp_id = data.get("emp_id")
        new_room = message.text.strip()
        await db.execute(
            "UPDATE employees SET room=$1 WHERE id=$2",
            new_room,
            emp_id
        )
        await message.answer(f"✅ Кабинет обновлён: {new_room}")
        await state.clear()

# -------------------- РЕДАКТИРОВАНИЕ КАБИНЕТА --------------------
@router.callback_query(F.data.startswith("edit_room_"))
async def edit_room(callback: CallbackQuery, state: FSMContext):
    emp_id = int(callback.data.split("_")[-1])
    await state.update_data(emp_id=emp_id, action="edit")
    await state.set_state(CabinetStates.entering_room)
    await callback.message.answer("Введите новый номер кабинета для сотрудника:")
    await callback.answer()

# -------------------- УДАЛЕНИЕ СОТРУДНИКА --------------------
@router.callback_query(F.data.startswith("delete_emp_"))
async def delete_employee(callback: CallbackQuery):
    emp_id = int(callback.data.split("_")[-1])
    await db.execute("DELETE FROM employees WHERE id=$1", emp_id)
    await callback.message.answer("✅ Сотрудник удалён.")
    await callback.answer()

# -------------------- ОТПРАВКА В ОСНОВНОЙ ЧАТ --------------------
@router.callback_query(F.data == "send_cabinets_main")
async def send_cabinets_main(callback: CallbackQuery):
    # Берём всех активных сотрудников
    employees = await db.fetch(
        "SELECT full_name, room FROM employees WHERE active=TRUE ORDER BY full_name"
    )

    if not employees:
        await callback.message.answer("Список сотрудников пуст.")
        await callback.answer()
        return

    text = "📋 <b>Кабинеты</b>\n\n"
    for emp in employees:
        text += f"{emp['full_name']} — {emp['room'] or 'Не указан'}\n"

    for chat_id in CABINET_GROUP_IDS:
        msg = await bot.send_message(chat_id, text)
    
   
    try:
        await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
    except Exception as e:

        await callback.answer("✅ Список отправлен в основной чат!")




# ===================== CREATE TASK IN GROUP =====================
@router.message(Command("задача"))
async def create_task_from_group(message: Message):
    # Только группы
    if message.chat.type not in ("group", "supergroup"):
        await message.reply("ℹ️ Используй кнопку «Добавить задачу» в ЛС")
        return

    task_text = message.text.replace("/задача", "", 1).strip()
    if not task_text:
        await message.reply("✍️ Напиши текст задачи:")
        return

    # 🔹 Создаём задачу
    row = await db.fetchrow(
        """
        INSERT INTO tasks (user_id, text, task_datetime, created_at, completed)
        VALUES ($1, $2, NULL, NOW(), FALSE)
        RETURNING id
        """,
        message.from_user.id,
        task_text
    )
    task_id = row["id"]

    # 🔹 Отправляем сообщение в группу
    msg = await message.answer(
        f"📌 <b>Задача</b>\n{task_text}",
        parse_mode="HTML"
    )

    # 🔹 Сохраняем message_id
    await db.execute(
        "UPDATE tasks SET task_message_id=$1 WHERE id=$2",
        msg.message_id,
        task_id
    )

# ===================== HANDLE + / ПРИНЯТО =====================
@router.message(F.reply_to_message)
async def handle_task_reply(message: Message):
    if not message.text:
        return

    text_lower = message.text.lower()
    reply = message.reply_to_message

    # 🔹 Найти задачу
    task = await db.fetchrow(
        "SELECT * FROM tasks WHERE task_message_id=$1 AND completed=FALSE",
        reply.message_id
    )

    if not task:
        return

    # ✅ Выполнить задачу
    if message.text == "+":
        await db.execute(
            "UPDATE tasks SET completed=TRUE, completed_at=NOW() WHERE id=$1",
            task["id"]
        )
        await message.reply("✅ Задача выполнена")
        return

    # 👤 Назначить исполнителя по reply + текст
    elif text_lower in ("принято", "принял", "беру"):
        await db.execute(
            "UPDATE tasks SET assigned_user_id=$1 WHERE id=$2",
            message.from_user.id,
            task["id"]
        )
        await message.reply(
            f"👤 Исполнитель назначен: <b>{message.from_user.full_name}</b>",
            parse_mode="HTML"
        )
        return

    # 👤 Назначение через @ник
    elif "@" in message.text:
        tag = message.text.strip().split()[0]
        if tag.startswith("@"):
            tag = tag[1:]

        if tag not in ALLOWED_ASSIGNEES:
            await message.reply("❌ Этот пользователь недоступен.")
            return

        assigned_id = ALLOWED_ASSIGNEES[tag]

        # Обновляем исполнителя
        await db.execute(
            "UPDATE tasks SET assigned_user_id=$1 WHERE id=$2",
            assigned_id,
            task["id"]
        )

        # Сообщение в группе
        await message.answer(
            f"✅ <b>Исполнитель назначен: @{tag}</b>",
            parse_mode="HTML"
        )

        # Сообщение в ЛС
        await bot.send_message(
            assigned_id,
            f"📌 <b>Тебе назначена задача:</b>\n\n{task['text']}",
            parse_mode="HTML"
        )
        return


from aiogram.types import Message, CallbackQuery
from aiogram import F
from datetime import datetime
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

@router.message(F.text == "/my_tasks") 
async def show_my_tasks(message: Message):
    user_id = message.from_user.id
    tasks = await db.fetch("SELECT id, text, task_datetime FROM tasks WHERE user_id=$1 AND completed=FALSE ORDER BY task_datetime", user_id)

    for t in tasks:
        dt = t['task_datetime'].strftime("%d.%m.%Y %H:%M")
        markup = InlineKeyboardMarkup().add(
            InlineKeyboardButton(text="✅ Выполнить", callback_data=f"complete_{t['id']}")
        )
        await message.answer(f"📝 {t['text']} — {dt}", reply_markup=markup)

# Обработчик кнопки "Выполнить"
@router.callback_query(F.data.startswith("complete_"))
async def complete_task_lm(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    await db.execute("UPDATE tasks SET completed=TRUE, completed_by=$1, completed_at=$2 WHERE id=$3",
                     user_id, datetime.now(), task_id)

    await callback.message.edit_text(callback.message.text + "\n\n✅ Выполнено")
    await callback.answer("Задача выполнена.")




async def send_tasks(callback: CallbackQuery, start: datetime, end: datetime):
    rows = await db.fetch(
        """
        SELECT * FROM tasks
        WHERE user_id=$1 AND task_datetime BETWEEN $2 AND $3
        ORDER BY task_datetime
        """,
        callback.from_user.id,
        start,
        end
    )

    if not rows:
        await callback.message.answer("Нет задач.")
        return

    for task in rows:
        dt = task["task_datetime"]
        date_text = dt.strftime("%d.%m.%Y %H:%M") if dt else "Без даты"

    await callback.message.answer(
        f"📌 <b>{task['text']}</b>\n"
        f"⏰ {date_text}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✏️ Редактировать",
                        callback_data=f"edit_{task['id']}"
                    ),
                    InlineKeyboardButton(
                        text="🗑 Удалить",
                        callback_data=f"del_{task['id']}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🔁 Изменить исполнителя",
                        callback_data=f"change_exec_{task['id']}"
                    )
                ]
            ]
        )
    )

    await callback.answer()



from datetime import datetime, timedelta
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ===================== ОБЩАЯ ФУНКЦИЯ ФОРМАТИРОВАНИЯ ТЕКСТА =====================
def format_task_text(task: dict):
    # Исполнитель
    executor_text = ""
    if task.get("assigned_user_id"):
        assigned_nick = None
        for nick, uid in ALLOWED_ASSIGNEES.items():
            if uid == task["assigned_user_id"]:
                assigned_nick = nick
                break
        if assigned_nick:
            executor_text = f"\n👤 Исполнитель: @{assigned_nick}"

    # Дата/время задачи
    dt_text = task["task_datetime"].strftime("%d.%m.%Y %H:%M") if task["task_datetime"] else "Не указано"

    return f"📌 <b>{task['text']}</b>{executor_text}\n⏰ {dt_text}"


# ===================== ОБЩАЯ ФУНКЦИЯ СОЗДАНИЯ КНОПОК =====================
def task_buttons(task: dict):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Выполнить", callback_data=f"done_{task['id']}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{task['id']}")
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_{task['id']}"),
            InlineKeyboardButton(text="🔁 Изменить исполнителя", callback_data=f"change_exec_{task['id']}")
        ]
    ])


# ===================== ФУНКЦИЯ ДЛЯ ВЫВОДА ЗАДАЧ =====================
async def send_tasks_for_day(callback: CallbackQuery, start_of_day: datetime, end_of_day: datetime, day_name: str):
    rows = await db.fetch(
        """
        SELECT *
        FROM tasks
        WHERE task_datetime BETWEEN $1 AND $2
          AND completed = FALSE
        ORDER BY task_datetime
        """,
        start_of_day, end_of_day
    )

    if not rows:
        await callback.message.answer(f"📭 На {day_name} нет задач.")
        await callback.answer()
        return

    for task in rows:
        text = format_task_text(task)
        markup = task_buttons(task)
        await callback.message.answer(text, reply_markup=markup, parse_mode="HTML")

    await callback.answer()


# ===================== Список задач на сегодня =====================
@router.callback_query(F.data == "today_tasks")
async def today_tasks(callback: CallbackQuery):
    now = datetime.now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    await send_tasks_for_day(callback, start_of_day, end_of_day, "сегодня")


# ===================== Список задач на завтра =====================
@router.callback_query(F.data == "tomorrow_tasks")
async def tomorrow_tasks(callback: CallbackQuery):
    tomorrow = datetime.now() + timedelta(days=1)
    start_of_day = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)
    await send_tasks_for_day(callback, start_of_day, end_of_day, "завтра")


@router.callback_query(F.data == "all_tasks")
async def all_tasks(callback: CallbackQuery):
    rows = await db.fetch("SELECT * FROM tasks WHERE completed=FALSE ORDER BY task_datetime")
    
    if not rows:
        await callback.message.answer("Нет активных задач.")
        return

    for task in rows:
        # исполнитель
        executor = " Не назначен"
        if task["assigned_user_id"]:
            executor = next(
                (tag for tag, uid in ALLOWED_ASSIGNEES.items() if uid == task["assigned_user_id"]),
                f"ID {task['assigned_user_id']}"
            )

        await callback.message.answer(
            f"🧑‍💼 <b>{task['text']}</b>\n"
            f"⏰ {task['task_datetime'].strftime('%d.%m.%Y %H:%M') if task['task_datetime'] else 'Без даты'}\n"
            f"👤 <b>Исполнитель:</b> @{executor}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{task['id']}"),
                    InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_{task['id']}")
                ],
                [
                    InlineKeyboardButton(text="🔁 Изменить исполнителя", callback_data=f"change_exec_{task['id']}")
                ]
            ])
        )
        


# ===================== COMPLETE TASK =====================

async def complete_task(task_id: int):
    await db.execute(
        "UPDATE tasks SET completed=TRUE WHERE id=$1",
        task_id
    )

    try:
        scheduler.remove_job(f"task_{task_id}")
    except:
        pass


@router.callback_query(F.data.startswith("done_"))
async def done_callback(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[1])

    await complete_task(task_id)

    await callback.message.answer("✅ Задача выполнена")
    await callback.answer()


@router.message(F.reply_to_message)
async def complete_by_reply(message: Message):
    if message.chat.id != GROUP_ID:
        return

    task = await db.fetchrow(
        "SELECT * FROM tasks WHERE group_msg_id=$1 AND assigned_user_id=$2 AND completed=FALSE",
        message.reply_to_message.message_id,
        message.from_user.id
    )

    if not task:
        return

    await complete_task(task["id"])
    await message.answer("✅ <b>Задача выполнена</b>")

# ===================== ASSIGNED TO ME =====================

@router.callback_query(F.data == "assigned_to_me")
async def assigned_to_me(callback: CallbackQuery):
    rows = await db.fetch(
        """
        SELECT * FROM tasks
        WHERE assigned_user_id=$1 AND completed=FALSE
        ORDER BY task_datetime NULLS LAST
        """,
        callback.from_user.id
    )

    if not rows:
        await callback.message.answer("📭 Нет назначенных задач.")
        return

    for task in rows:
        await callback.message.answer(
            f"📌 <b>{task['text']}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Выполнено", callback_data=f"done_{task['id']}")]
            ])
        )

    await callback.answer()

# ===================== REMINDERS =====================

async def remind_task(task_id: int):
    task = await db.fetchrow(
        "SELECT id, text, completed FROM tasks WHERE id=$1",
        task_id
    )

    if not task or task["completed"]:
        return

    await send_message_safe(
        GROUP_ID,
        f"⏰ <b>Напоминание</b>\n\n{task['text']}"
    )


    text = (
        "📌 Задача:\n"
        f"{task['text']}\n\n"
        f"⏰ {task['task_datetime'].strftime('%d.%m.%Y %H:%M')}\n\n"
    )

    await bot.send_message(
        task["target_chat_id"],
        text
    )



def schedule_reminder(task_id: int):
    scheduler.add_job(
        remind_task,
        "interval",
        hours=1,
        args=[task_id],
        id=f"task_{task_id}",
        replace_existing=True
    )



@router.callback_query(F.data == "my_tasks")
async def my_tasks(callback: CallbackQuery):
    rows = await db.fetch(
        """
        SELECT * FROM tasks
        WHERE user_id=$1 AND completed=FALSE
        ORDER BY task_datetime NULLS LAST
        """,
        callback.from_user.id
    )

    if not rows:
        await callback.message.answer("У тебя нет активных задач.")
        return

    for task in rows:
        if task["assigned_user_id"]:
            executor = next(
                (tag for tag, uid in ALLOWED_ASSIGNEES.items() if uid == task["assigned_user_id"]),
                f"ID {task['assigned_user_id']}"
            )
        else:
            executor = "Не назначен"

        await callback.message.answer(
            f"🧑‍💼 <b>{task['text']}</b>\n"
            f"⏰ {task['task_datetime'].strftime('%d.%m.%Y %H:%M') if task['task_datetime'] else 'Без даты'}\n"
            f"👤 <b>Исполнитель:</b> @{executor}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_{task['id']}"),
                    InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_{task['id']}")
                ],
                [
                    InlineKeyboardButton(text="🔁 Изменить исполнителя", callback_data=f"change_exec_{task['id']}")
                ]
            ])
        )
    await callback.answer()

    

@router.message(EditDateFSM.waiting_time)
async def save_new_datetime(message: Message, state: FSMContext, db, bot: Bot):
    # Проверка формата времени
    try:
        hour, minute = map(int, message.text.split(":"))
    except ValueError:
        await message.answer("❌ Формат времени: ЧЧ:ММ")
        return

    data = await state.get_data()
    task_id = data["task_id"]

    # Собираем datetime
    new_dt = datetime.strptime(f"{data['date']} {hour}:{minute}", "%Y-%m-%d %H:%M")

    # Обновляем базу
    await db.execute(
        "UPDATE tasks SET task_datetime=$1 WHERE id=$2",
        new_dt,
        task_id
    )

    # Перепланируем отложенную отправку в группу
    scheduler.add_job(
        bot.send_message,
        "date",
        run_date=new_dt,
        args=[GROUP_ID, f"⏰ <b>Обновлённая задача</b>"],
        id=f"delayed_{task_id}",
        replace_existing=True
    )


    task = await db.fetchrow(
    "SELECT text, task_datetime FROM tasks WHERE id=$1",
    task_id
)
    text = (
    "📌 Задача:\n"
    f"{task['text']}\n\n"
    f"⏰ {task['task_datetime'].strftime('%d.%m.%Y %H:%M')}"
)
    await bot.send_message(
    GROUP_ID,
    text
)
# новая дата задачи
    await db.execute(
    """
    UPDATE tasks
    SET task_datetime = $1,
        next_send_at = $1
    WHERE id = $2
    """,
    new_dt,
    task_id
)


    await message.answer(f"✅ <b>Дата и время обновлены:</b>\n{new_dt.strftime('%d.%m.%Y %H:%M')}")
    await state.clear()



@router.callback_query(F.data.startswith("change_exec_"))
async def change_executor(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[-1])

    kb = [
        [InlineKeyboardButton(text=tag, callback_data=f"set_exec_{task_id}_{uid}")]
        for tag, uid in ALLOWED_ASSIGNEES.items()
    ]

    await callback.message.answer(
        "Выбери нового исполнителя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("set_exec_"))
async def set_executor(callback: CallbackQuery):
    _, _, task_id, uid = callback.data.split("_")
    task_id = int(task_id)
    uid = int(uid)

    await db.execute(
        "UPDATE tasks SET assigned_user_id=$1 WHERE id=$2",
        uid,
        task_id
    )

    await bot.send_message(uid, "📌 Тебе назначили новую задачу.")
    await callback.message.answer("✅ Исполнитель изменён")
    await callback.answer()

@router.callback_query(F.data.startswith("edit_"))
async def edit_task(callback: CallbackQuery, state: FSMContext):
    
    parts = callback.data.split("_")

    if len(parts) == 2:
        try:
            task_id = int(parts[1])
        except ValueError:
            await callback.answer("Ошибка! Некорректный ID задачи.", show_alert=True)
            return
        await state.update_data(task_id=task_id)

        await callback.message.answer(
            "Что ты хочешь отредактировать?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✍️ Текст задачи", callback_data=f"edit_text_{task_id}")],
                [InlineKeyboardButton(text="📅 Дату и время", callback_data=f"edit_datetime_{task_id}")]
            ])
        )
        await callback.answer()
        return

    # Новый формат для кнопок внутри меню "Редактировать задачу"
    if len(parts) == 3:
        action = parts[1]      # text или datetime
        try:
            task_id = int(parts[2])
        except ValueError:
            await callback.answer("Ошибка! Некорректный ID задачи.", show_alert=True)
            return

        await state.update_data(task_id=task_id)

        if action == "text":
            await callback.message.answer("✍️ Введи новый текст задачи:")
            await state.set_state(EditTaskFSM.waiting_text)
        elif action == "datetime":
            await callback.message.answer("📅 Введи дату и время задачи в формате ДД.ММ.ГГГГ")
            await state.set_state(EditTaskFSM.waiting_date)

        await callback.answer()
        return
    # Создаём календарь на выбранный месяц
def get_calendar(year: int, month: int) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=7)

    # Заголовок с навигацией
    prev_month = (datetime(year, month, 1) - timedelta(days=1))
    next_month = (datetime(year, month, 28) + timedelta(days=4))  # точно переходит на следующий месяц
    markup.row(
        InlineKeyboardButton("⬅️", callback_data=f"change_month_{prev_month.year}_{prev_month.month}"),
        InlineKeyboardButton(f"{month}.{year}", callback_data="ignore"),
        InlineKeyboardButton("➡️", callback_data=f"change_month_{next_month.year}_{next_month.month}")
    )

    # Дни недели
    markup.row(*[InlineKeyboardButton(d, callback_data="ignore") for d in "Пн Вт Ср Чт Пт Сб Вс".split()])

    # Кнопки дней
    first_weekday, days_in_month = calendar.monthrange(year, month)
    buttons = []

    # Пустые кнопки для сдвига начала месяца
    for _ in range((first_weekday + 6) % 7):
        buttons.append(InlineKeyboardButton(" ", callback_data="ignore"))

    for day in range(1, days_in_month + 1):
        buttons.append(InlineKeyboardButton(str(day), callback_data=f"calendar_{year}_{month}_{day}"))

    markup.add(*buttons)
    return markup


# Нажатие на "Дату и время"
@router.callback_query(F.data.startswith("edit_datetime"))
async def edit_datetime(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    task_id = data.get("task_id")
    now = datetime.now()

    await callback.message.answer(
        "Выбери новую дату для задачи:",
        reply_markup=get_calendar(now.year, now.month)
    )
    await callback.answer()


# Листание месяцев
@router.callback_query(F.data.startswith("change_month_"))
async def change_month(callback: CallbackQuery, state: FSMContext):
    _, year, month = callback.data.split("_")[2:]
    year, month = int(year), int(month)

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month))
    await callback.answer()


# Выбор конкретного дня
@router.callback_query(F.data.startswith("calendar_"))
async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")
    new_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"  # YYYY-MM-DD
    await state.update_data(date=new_date)

    await callback.message.answer(
        f"Выбрана дата: {day}.{month}.{year}\nТеперь введи время в формате ЧЧ:ММ"
    )
    await state.set_state(EditDateFSM.waiting_time)
    await callback.answer()



# Сохраняем новый текст
@router.message(EditTaskFSM.waiting_text)
async def save_new_text(message: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["task_id"]
    new_text = message.text

    await db.execute(
        "UPDATE tasks SET text=$1 WHERE id=$2",
        new_text,
        task_id
    )

    await message.answer(f"✅ Текст задачи обновлён:\n{new_text}")
    await state.clear()

# Сохраняем новую дату
@router.message(EditTaskFSM.waiting_date)
async def save_new_date(message: Message, state: FSMContext):
    try:
        day, month, year = map(int, message.text.split("."))
    except ValueError:
        await message.answer("❌ Формат даты: ДД.ММ.ГГГГ")
        return

    new_date = f"{year}-{month:02d}-{day:02d}"
    await state.update_data(date=new_date)

    await message.answer("⏰ Введи время задачи в формате ЧЧ:ММ")
    await state.set_state(EditTaskFSM.waiting_time)

# Сохраняем новое время и обновляем дату и время
@router.message(EditTaskFSM.waiting_time)
async def save_new_datetime(message: Message, state: FSMContext):
    try:
        hour, minute = map(int, message.text.split(":"))
    except ValueError:
        await message.answer("❌ Формат времени: ЧЧ:ММ")
        return

    data = await state.get_data()
    task_id = data["task_id"]
    new_dt_str = f"{data['date']} {hour:02d}:{minute:02d}"
    
    from datetime import datetime
    new_dt = datetime.strptime(new_dt_str, "%Y-%m-%d %H:%M")

    await db.execute(
        "UPDATE tasks SET task_datetime=$1 WHERE id=$2",
        new_dt,
        task_id
    )

    # Перепланируем отложенную отправку
    scheduler.add_job(
        bot.send_message,
        "date",
        run_date=new_dt,
        args=[GROUP_ID, f"⏰ <b>Обновлённая задача</b>"],
        id=f"delayed_{task_id}",
        replace_existing=True
    )



    task = await db.fetchrow(
    "SELECT text, task_datetime FROM tasks WHERE id=$1",
    task_id
)
    text = (
    "📌 Задача:\n"
    f"{task['text']}\n\n"
    f"⏰ {task['task_datetime'].strftime('%d.%m.%Y %H:%M')}"
)
    await bot.send_message(
    GROUP_ID,
    text
)
# новая дата задачи
    await db.execute(
    """
    UPDATE tasks
    SET task_datetime = $1,
        next_send_at = $1
    WHERE id = $2
    """,
    new_dt,
    task_id
)


    await message.answer(f"✅ Дата и время обновлены:\n{new_dt.strftime('%d.%m.%Y %H:%M')}")
    await state.clear()

@router.callback_query(F.data.startswith("edit_text"), F.data.startswith("edit_datetime"))
async def save_task_changes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    task_id = data.get("task_id")
    task_text = data.get("task_text")  # обновлённый текст
    task_datetime = data.get("task_datetime")  # обновлённая дата/время

    # Обновляем в базе
    await db.execute(
        "UPDATE tasks SET text=$1, datetime=$2 WHERE id=$3",
        task_text, task_datetime, task_id
    )

    
    # Формируем текст для группы
    new_message_text = (
        f"Задача обновлена!\n\n"
        f"Текст: {task_text}\n"
        f"Дата и время: {task_datetime.strftime('%d.%m.%Y %H:%M')}"
    )

    # Отправляем в группу (или редактируем существующее сообщение)
    group_chat_id = GROUP_ID 
    root_message_id = data.get("root_message_id")  
    if root_message_id:
        await bot.edit_message_text(
            chat_id=group_chat_id,
            message_id=root_message_id,
            text=new_message_text
        )
    else:
        msg = await bot.send_message(chat_id=group_chat_id, text=new_message_text)
        # Сохраняем message_id для последующих обновлений
        await db.execute("UPDATE tasks SET root_message_id=$1 WHERE id=$2", msg.message_id, task_id)

    await callback.answer("Задача обновлена!")


@router.callback_query(F.data.startswith("del_"))
async def delete_task(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[1])

    await db.execute("DELETE FROM tasks WHERE id=$1", task_id)

    try:
        scheduler.remove_job(f"task_{task_id}")
    except:
        pass

    await callback.message.answer("🗑 Задача удалена")
    await callback.answer()

@router.callback_query(F.data == "all_tasks")
async def all_tasks(callback: CallbackQuery):
    rows = await db.fetch(
        """
        SELECT * FROM tasks
        WHERE user_id=$1
        ORDER BY task_datetime
        """,
        callback.from_user.id
    )

    if not rows:
        await callback.message.answer("Нет задач.")
        return

    current_month = None
    for task in rows:
        month = task["task_datetime"].strftime("%B %Y") if task["task_datetime"] else "Без даты"
        if month != current_month:
            current_month = month
            await callback.message.answer(f"📅 <b>{month}</b>")

        await callback.message.answer(
            f"• {task['text']} ({task['task_datetime'].strftime('%d.%m %H:%M') if task['task_datetime'] else ''})"
        )

    await callback.answer()
    

@router.message(Command("edit"))
async def edit_cmd(message: Message):
    await message.answer("✏️ Используй кнопки редактирования в списке задач.")


@router.message(Command("delete"))
async def delete_cmd(message: Message):
    await message.answer("🗑 Используй кнопки удаления в списке задач.")

ADMIN_ID = 335256810

@router.message(F.text == "/архив")
async def archive_tasks(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ У тебя нет доступа к архиву")
        return

    tasks = await db.fetch(
        "SELECT id, text, user_id, completed_at FROM tasks WHERE completed=TRUE ORDER BY completed_at DESC"
)

    archive_text = ""
    for t in tasks:
        dt = t['completed_at'].strftime("%d.%m.%Y %H:%M") if t['completed_at'] else "неизвестно"
        archive_text += f"✅ Задача: {t['text']}\nПользователь: {t['user_id']}\nВыполнена: {dt}\n\n"

    await message.answer(archive_text or "Архив пуст.")

async def send_message_safe(chat_id: int, text: str):
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML
    )

    import asyncio
from datetime import datetime, timedelta
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Глобальная переменная, чтобы не запускать scheduler дважды
scheduler_started = False

async def task_scheduler():
    global scheduler_started
    if scheduler_started:
        return
    scheduler_started = True

    while True:
        try:
            now = datetime.now()

            tasks = await db.fetch(
                """
                SELECT *
                FROM tasks
                WHERE completed = FALSE
                  AND next_send_at <= $1
                ORDER BY next_send_at ASC
                """,
                now
            )

            for task in tasks:
                task_datetime = task["task_datetime"]      # может быть None
                next_send = task["next_send_at"]           # всегда есть

                # 1️⃣ Если у задачи есть дата, но она ещё не наступила — ждём
                if task_datetime and next_send < task_datetime:
                    continue

                send_time = next_send

                # ---- ТЕКСТ ----
                dt_text = (
                    task_datetime.strftime("%d.%m.%Y %H:%M")
                    if task_datetime else "Без даты"
                )

                executor_text = ""
                if task.get("assigned_user_id"):
                    assigned_nick = next(
                        (nick for nick, uid in ALLOWED_ASSIGNEES.items()
                         if uid == task["assigned_user_id"]),
                        None
                    )
                    if assigned_nick:
                        executor_text = f"\n👤 Исполнитель: @{assigned_nick}"

                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        InlineKeyboardButton(
                            text="✅ Выполнить",
                            callback_data=f"done_{task['id']}"
                        )
                    ]]
                )

                sent_any = False

                # 2️⃣ Catch-up — шлём ВСЕ пропущенные часы
                while send_time <= now:
                    text = (
                        f"⏰ <b>Напоминание о задаче</b>\n"
                        f"📌 {task['text']}{executor_text}\n"
                        f"🗓 {dt_text}\n"
                        f"⏱ Запланировано: {send_time.strftime('%H:%M')}"
                    )

                    await bot.send_message(
                        chat_id=GROUP_ID,
                        text=text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )

                    send_time += timedelta(hours=1)
                    sent_any = True

                # 3️⃣ Сохраняем СТРОГО будущее время
                if sent_any:
                    await db.execute(
                        "UPDATE tasks SET next_send_at=$1 WHERE id=$2",
                        send_time,
                        task["id"]
                    )

        except Exception:
            import traceback
            print(traceback.format_exc())
            await asyncio.sleep(10)

        # Ждём 30 секунд перед следующей итерацией
        await asyncio.sleep(30)

    
        

@router.message(Command("задача"))
async def create_task_from_allowed_groups(message: Message):
    # ❌ если не группа
    if message.chat.type not in ("group", "supergroup"):
        return

    # ❌ если группа не разрешена
    if message.chat.id not in ALLOWED_TASK_GROUPS:
        await message.reply("❌ В этой группе нельзя создавать задачи.")
        return

    task_text = message.text.replace("/задача", "", 1).strip()
    if not task_text:
        await message.reply("✍️ Напиши текст задачи после /задача")
        return

    # ✅ создаём задачу
    row = await db.fetchrow(
        """
        INSERT INTO tasks (user_id, text, created_at, completed)
        VALUES ($1, $2, NOW(), FALSE)
        RETURNING id
        """,
        message.from_user.id,
        task_text
    )

    task_id = row["id"]

    # ✅ отправляем ТОЛЬКО в корневую группу
    msg = await bot.send_message(
        ROOT_GROUP_ID,
        f"📌 <b>Задача</b>\n{task_text}",
        parse_mode="HTML"
    )

    # сохраняем message_id
    await db.execute(
        "UPDATE tasks SET task_message_id=$1 WHERE id=$2",
        msg.message_id,
        task_id
    )

    await message.reply("✅ Задача создана")

async def send_to_cabinets(text: str):
    for chat_id in CABINET_GROUP_IDS:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML"
        )


# ===================== HANDLE + / ПРИНЯТО НА ЛЮБОЕ СООБЩЕНИЕ ЗАДАЧИ =====================
@router.message(
    F.reply_to_message,
    F.text.in_(["+", "принято", "Принято", "принял", "беру"])
)
async def handle_task_accept_or_done(message: Message):

    replied_msg_id = message.reply_to_message.message_id

    # Ищем задачу по last_message_id
    task = await db.fetchrow(
        """
        SELECT *
        FROM tasks
        WHERE last_message_id = $1
          AND completed = FALSE
        """,
        replied_msg_id
    )

    if not task:
        return  # это не сообщение задачи или она уже выполнена

    # ===================== ВЫПОЛНЕНИЕ ЗАДАЧИ =====================
    if message.text.strip() == "+":
        await db.execute(
            """
            UPDATE tasks
            SET completed = TRUE,
                completed_at = NOW()
            WHERE id = $1
            """,
            task["id"]
        )

        await message.reply("✅ Задача выполнена")
        return

    # ===================== НАЗНАЧЕНИЕ ИСПОЛНИТЕЛЯ =====================
    if message.text.lower() in ("принято", "принял", "беру"):
        # если уже есть исполнитель — ничего не делаем
        if task["assigned_user_id"]:
            return

        await db.execute(
            """
            UPDATE tasks
            SET assigned_user_id = $1
            WHERE id = $2
            """,
            message.from_user.id,
            task["id"]
        )

        await message.reply("👤 Задача принята")

        dt_text = (
    task["task_datetime"].strftime("%d.%m.%Y %H:%M")
    if task["task_datetime"]
    else "Без даты"
)

    executor = ""
    if task.get("assigned_user_id"):
        for nick, uid in ALLOWED_ASSIGNEES.items():
            if uid == task["assigned_user_id"]:
                executor = f"\n👤 Исполнитель: @{nick}"
            break

    text = (
    f"📌 <b>{task['text']}</b>"
    f"{executor}\n"
    f"🕒 {dt_text}"
)

    msg = await bot.send_message(
    chat_id=GROUP_ID,
    text=text,
    parse_mode="HTML"
)

    await db.execute(
    "UPDATE tasks SET last_message_id=$1 WHERE id=$2",
    msg.message_id,
    task["id"]
)
    try:            
        await bot.send_message(...)
    except TelegramForbiddenError:
        print("Пользователь закрыл ЛС")
    except Exception as e:
        print(f"Ошибка при отправке ЛС: {e}")

# ===================== HANDLE + / ПРИНЯТО НА ЛЮБОЕ СООБЩЕНИЕ ЗАДАЧИ =====================
@router.message(
    F.reply_to_message,
    F.text.in_(["+", "принято", "Принято", "принял", "беру"])
)
async def handle_task_accept_or_done(message: Message):

    replied_msg_id = message.reply_to_message.message_id

    # Ищем задачу по last_message_id
    task = await db.fetchrow(
        """
        SELECT *
        FROM tasks
        WHERE last_message_id = $1
          AND completed = FALSE
        """,
        replied_msg_id
    )

    if not task:
        return  # это не сообщение задачи или она уже выполнена

    # ===================== ВЫПОЛНЕНИЕ ЗАДАЧИ =====================
    if message.text.strip() == "+":
        await db.execute(
            """
            UPDATE tasks
            SET completed = TRUE,
                completed_at = NOW()
            WHERE id = $1
            """,
            task["id"]
        )

        await message.reply("✅ Задача выполнена")
        return

    # ===================== НАЗНАЧЕНИЕ ИСПОЛНИТЕЛЯ =====================
    if message.text.lower() in ("принято", "принял", "беру"):
        # если уже есть исполнитель — ничего не делаем
        if task["assigned_user_id"]:
            return

        await db.execute(
            """
            UPDATE tasks
            SET assigned_user_id = $1
            WHERE id = $2
            """,
            message.from_user.id,
            task["id"]
        )

        await message.reply("👤 Задача принята")

        # уведомление в ЛС
        try:
            dt_text = (
                task["task_datetime"].strftime("%d.%m.%Y %H:%M")
                if task["task_datetime"]
                else "Без даты"
            )

            await bot.send_message(
                message.from_user.id,
                f"📌 <b>Тебе назначена задача:</b>\n\n"
                f"{task['text']}\n"
                f"🕒 {dt_text}",
                parse_mode="HTML"
            )
        except:
            pass

# ===================== START =====================

async def main():
    await init_db()
    scheduler.start()
    asyncio.create_task(task_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
