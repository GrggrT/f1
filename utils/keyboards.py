from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

from data.models import Driver, Constructor


# ── Menu button labels ──

MENU_TEAM = "\U0001f3ce Команда"
MENU_STANDINGS = "\U0001f4ca Таблица"
MENU_RACE = "\U0001f3c1 Гонка"
MENU_PREDICTIONS = "\U0001f52e Прогнозы"
MENU_SURVIVOR = "\u2620\ufe0f Survivor"
MENU_CHIPS = "\U0001f0cf Чипы"
MENU_PRICES = "\U0001f4b0 Цены"
MENU_RULES = "\U0001f4cb Правила"
MENU_HELP = "\u2753 Помощь"

ALL_MENU_TEXTS = [
    MENU_TEAM, MENU_STANDINGS, MENU_RACE, MENU_PREDICTIONS,
    MENU_SURVIVOR, MENU_CHIPS, MENU_PRICES, MENU_RULES, MENU_HELP,
]

TEAM_EMOJIS = {
    "red_bull": "\U0001f535",
    "mclaren": "\U0001f7e0",
    "ferrari": "\U0001f534",
    "mercedes": "\u26aa",
    "aston_martin": "\U0001f7e2",
    "alpine": "\U0001f535",
    "williams": "\U0001f535",
    "racing_bulls": "\U0001f535",
    "haas": "\u26aa",
    "audi": "\u26ab",
    "cadillac": "\U0001f7e2",
}


def build_main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(MENU_TEAM), KeyboardButton(MENU_STANDINGS), KeyboardButton(MENU_RACE)],
        [KeyboardButton(MENU_PREDICTIONS), KeyboardButton(MENU_SURVIVOR), KeyboardButton(MENU_CHIPS)],
        [KeyboardButton(MENU_PRICES), KeyboardButton(MENU_RULES), KeyboardButton(MENU_HELP)],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, is_persistent=True)


def _short_name(full_name: str) -> str:
    parts = full_name.split()
    return parts[-1] if parts else full_name


def build_driver_selection_keyboard(
    drivers: list[Driver],
    selected: list[str],
    budget_remaining: float,
) -> InlineKeyboardMarkup:
    sorted_drivers = sorted(drivers, key=lambda x: -x.price)

    buttons = []
    row: list[InlineKeyboardButton] = []
    for d in sorted_drivers:
        is_selected = d.id in selected
        can_afford = d.price <= budget_remaining or is_selected
        emoji = TEAM_EMOJIS.get(d.team, "\u26aa")
        name = _short_name(d.name)
        price_str = f"${d.price:.0f}M"

        if is_selected:
            text = f"\u2705 {name} {price_str}"
        elif not can_afford:
            text = f"\u274c {name} {price_str}"
        else:
            text = f"{emoji} {name} {price_str}"

        row.append(InlineKeyboardButton(text, callback_data=f"pd_{d.id}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav = []
    if selected:
        nav.append(InlineKeyboardButton("\U0001f504 \u0421\u0431\u0440\u043e\u0441\u0438\u0442\u044c", callback_data="pd_reset"))
    if len(selected) == 5:
        nav.append(InlineKeyboardButton("\u27a1\ufe0f \u0414\u0430\u043b\u0435\u0435", callback_data="pd_next"))
    else:
        nav.append(InlineKeyboardButton(f"\u0412\u044b\u0431\u0440\u0430\u043d\u043e {len(selected)}/5", callback_data="pd_count"))
    buttons.append(nav)

    return InlineKeyboardMarkup(buttons)


def build_constructor_keyboard(
    constructors: list[Constructor],
    budget_remaining: float,
) -> InlineKeyboardMarkup:
    sorted_constructors = sorted(constructors, key=lambda x: -x.price)
    buttons = []
    row: list[InlineKeyboardButton] = []
    for c in sorted_constructors:
        can_afford = c.price <= budget_remaining
        emoji = TEAM_EMOJIS.get(c.id, "\u26aa")
        price_str = f"${c.price:.0f}M"
        if not can_afford:
            text = f"\u274c {c.name} {price_str}"
        else:
            text = f"{emoji} {c.name} {price_str}"
        row.append(InlineKeyboardButton(text, callback_data=f"pc_{c.id}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434 \u043a \u043f\u0438\u043b\u043e\u0442\u0430\u043c", callback_data="pc_back")])
    return InlineKeyboardMarkup(buttons)


def build_turbo_keyboard(selected_drivers: list[Driver]) -> InlineKeyboardMarkup:
    buttons = []
    row: list[InlineKeyboardButton] = []
    for d in selected_drivers:
        row.append(InlineKeyboardButton(f"\u26a1 {_short_name(d.name)}", callback_data=f"tb_{d.id}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434 \u043a \u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440\u0443", callback_data="tb_back")])
    return InlineKeyboardMarkup(buttons)


def build_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u2705 \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u044c", callback_data="team_confirm"),
        ],
        [
            InlineKeyboardButton("\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="team_back_turbo"),
            InlineKeyboardButton("\U0001f504 \u0417\u0430\u043d\u043e\u0432\u043e", callback_data="team_restart"),
        ],
    ])


def build_chips_keyboard(available: list[str]) -> InlineKeyboardMarkup:
    chip_names = {
        "WILDCARD": "\U0001f0cf Wildcard",
        "TRIPLE_BOOST": "\U0001f4a5 Triple Boost (3x)",
        "NO_NEGATIVE": "\U0001f6e1 No Negative",
    }
    buttons = [
        [InlineKeyboardButton(chip_names.get(c, c), callback_data=f"chip_{c}")]
        for c in available
    ]
    if not buttons:
        buttons = [[InlineKeyboardButton("\u0412\u0441\u0435 \u0447\u0438\u043f\u044b \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u044b", callback_data="chip_none")]]
    return InlineKeyboardMarkup(buttons)


def build_prediction_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u2705 \u0414\u0430", callback_data="pred_yes"),
            InlineKeyboardButton("\u274c \u041d\u0435\u0442", callback_data="pred_no"),
        ]
    ])


def build_confidence_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(i), callback_data=f"conf_{i}") for i in range(1, 6)]
    ])


def build_survivor_keyboard(
    available_drivers: list[Driver],
    used_ids: list[str],
) -> InlineKeyboardMarkup:
    buttons = []
    for d in sorted(available_drivers, key=lambda x: -x.price):
        if d.id in used_ids:
            text = f"\u274c {d.name} (\u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d)"
            buttons.append([InlineKeyboardButton(text, callback_data="surv_used")])
        else:
            text = f"\U0001f7e2 {d.name}"
            buttons.append([InlineKeyboardButton(text, callback_data=f"sv_{d.id}")])
    return InlineKeyboardMarkup(buttons)
