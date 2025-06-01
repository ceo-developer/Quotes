import telebot
from telebot import types
import aiohttp
from io import BytesIO
import threading
from threading import Lock
import time
import json
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image, ImageDraw, ImageFont
import textwrap
import os
import sys
import datetime
import random
from dotenv import load_dotenv
import asyncio
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),  # Relative path
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
logger.info("Loading environment variables...")
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))

if not all([TOKEN, API_ID, API_HASH, BOT_OWNER_ID]):
    logger.error("Missing environment variables")
    sys.exit(1)

SESSION_NAME = "MvChat25Bot"
bot = telebot.TeleBot(TOKEN)
app = Client(SESSION_NAME, bot_token=TOKEN, api_id=API_ID, api_hash=API_HASH)

# Global event loop
main_loop = None

# Data structures
chat_settings = {}
subscribed_chats = set()
reaction_counts = {}
user_reactions = {}
chat_schedules = {}
last_quote_times = {}
leaderboard_data = {}
latest_quotes = {}
quote_counts = {}
total_quote_count = 0
total_quote_count_lock = Lock()
start_message_ids = {}
help_message_ids = {}
welcome_messages = {}
command_cooldowns = {}
user_stats = {}  # {user_id: {chat_id: {join_date, reactions_given, quote_requests}}}
COOLDOWN_SECONDS = 30
DATA_FILE = "bot_data.json"

# Load persistent data
def load_data():
    global leaderboard_data, quote_counts, chat_settings, total_quote_count, user_stats
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
                leaderboard_data = data.get('leaderboard_data', {})
                quote_counts = data.get('quote_counts', {})
                chat_settings = data.get('chat_settings', {})
                total_quote_count = data.get('total_quote_count', 0)
                user_stats = data.get('user_stats', {})
            logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Data load error: {e}")

def save_data():
    try:
        with open(DATA_FILE, 'w') as f:
            json.dump({
                'leaderboard_data': leaderboard_data,
                'quote_counts': quote_counts,
                'chat_settings': chat_settings,
                'total_quote_count': total_quote_count,
                'user_stats': user_stats
            }, f)
        logger.info("Data saved")
    except Exception as e:
        logger.error(f"Data save error: {e}")

# Photos for /start and /help
photos = [
    "https://telegra.ph/file/1dbe35446e72cbb69fa1b.jpg",
    "https://telegra.ph/file/3f42d9aebe57894a5ef36.jpg",
    "https://telegra.ph/file/5bb4723d4c9271a2d626b.jpg",
    "https://telegra.ph/file/75bb8bb0d0097b64f163e.jpg",
    "https://telegra.ph/file/0c9de0690b9f0e4312531.jpg",
    "https://telegra.ph/file/b564108627b77e5bf1238.jpg",
    "https://telegra.ph/file/77c7580b6eeadf8afacc6.jpg",
    "https://telegra.ph/file/c5a9dd3e18e3a8c96575a.jpg",
    "https://telegra.ph/file/c0bf8c9cc6779af100feb.jpg",
    "https://telegra.ph/file/f73aaaca9b4279599cb2d.jpg",
    "https://telegra.ph/file/4c837e6236c6078588ff8.jpg",
    "https://telegra.ph/file/84bc7c501e237a799e153.jpg",
    "https://telegra.ph/file/c3e2b098a125b367d5fbd.jpg",
    "https://telegra.ph/file/58a29f64df38ae4fd74e4.jpg",
    "https://telegra.ph/file/e9442422dd2036540285d.jpg",
    "https://telegra.ph/file/461a2db8e3fd0364f8076.jpg",
    "https://telegra.ph/file/3d41afeadb9518c296d14.jpg",
    "https://telegra.ph/file/4003fb271c346589d977c.jpg",
    "https://telegra.ph/file/eed0ffcf4419f9fa11d98.jpg",
    "https://telegra.ph/file/0aed69b71e49b8625d854.jpg",
    "https://telegra.ph/file/a0011015d0d1e3aa2e3bb.jpg",
    "https://telegra.ph/file/248285fddcfff51321ca3.jpg",
    "https://telegra.ph/file/1165e5a0212dfc82b8398.jpg",
    "https://telegra.ph/file/4955ad1ac6a202251dae7.jpg",
    "https://telegra.ph/file/277fc609cf4ccdd1cefaf.jpg"
]

# Reactions
REACTIONS = {
    'like': '👍',
    'love': '❤️',
    'laugh': '😂',
    'wow': '😮',
    'dislike': '👎',
}

# Font Setup
font_paths = [
    "fonts/NotoSansDevanagari-Regular.ttf",
    "fonts/arial.ttf",
    "fonts/DejaVuSans.ttf",
    "fonts/DroidSans.ttf"
]

def load_font(font_path, size):
    try:
        if os.path.exists(font_path):
            return ImageFont.truetype(font_path, size)
        return None
    except Exception:
        return None

hindi_font = None
english_font = None
title_font = None
content_font = None

for font_path in font_paths:
    if hindi_font is None:
        hindi_font = load_font(font_path, 40)
    if english_font is None:
        english_font = load_font(font_path, 40)
    if title_font is None:
        title_font = load_font(font_path, 60)
    if content_font is None:
        content_font = load_font(font_path, 40)

if hindi_font is None or english_font is None or title_font is None or content_font is None:
    try:
        hindi_font = hindi_font or ImageFont.load_default()
        english_font = english_font or ImageFont.load_default()
        title_font = title_font or ImageFont.load_default()
        content_font = content_font or ImageFont.load_default()
        logger.warning("Using default fonts")
    except Exception as e:
        logger.error(f"Font loading error: {e}")
        hindi_font = None

# Quote Fetch
async def get_hindi_quote():
    logger.info("Fetching Hindi quote...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://hindi-quotes.vercel.app/random") as res:
                if res.status == 200:
                    data = await res.json()
                    quote = f"{data.get('quote', '')}\n\n— {data.get('type', 'अनजान')}"
                    logger.info("Quote fetched successfully")
                    return quote
                return None
    except Exception as e:
        logger.error(f"Error fetching quote: {e}")
        return None

# Image Quote Generator
def generate_quote_image(quote_text):
    logger.info("Generating quote image...")
    try:
        img = Image.new('RGB', (1080, 1080), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)

        title = "🌟 आज का विचार 🌟"
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_w = title_bbox[2] - title_bbox[0]
        draw.text(((1080 - title_w)/2, 100), title, font=title_font, fill=(0, 0, 0))

        parts = quote_text.split('\n\n— ')
        quote = parts[0]
        author = "— " + parts[1] if len(parts) > 1 else ""

        y_position = 300
        for line in quote.split('\n'):
            wrapped_lines = textwrap.wrap(line, width=30)
            for wrapped_line in wrapped_lines:
                bbox = draw.textbbox((0, 0), wrapped_line, font=content_font)
                w = bbox[2] - bbox[0]
                draw.text(((1080 - w)/2, y_position), wrapped_line, 
                         font=content_font, fill=(50, 50, 50))
                y_position += 60

        if author:
            bbox = draw.textbbox((0, 0), author, font=content_font)
            w = bbox[2] - bbox[0]
            draw.text((1080 - w - 100, 800), author, 
                     font=content_font, fill=(100, 100, 100))

        draw.line([(100, 900), (980, 900)], fill=(200, 200, 200), width=2)

        watermark = "© H2I Quotes"
        bbox = draw.textbbox((0, 0), watermark, font=content_font)
        w = bbox[2] - bbox[0]
        draw.text((1080 - w - 50, 950), watermark, 
                 font=content_font, fill=(150, 150, 150))

        img_bytes = BytesIO()
        img.save(img_bytes, format='JPEG', quality=95)
        img_bytes.seek(0)
        logger.info("Quote image generated successfully")
        return img_bytes
    except Exception as e:
        logger.error(f"Image generation failed: {e}")
        return None

# Reaction Button Creation
def create_reaction_buttons(chat_id, message_id):
    markup = types.InlineKeyboardMarkup()
    counts = reaction_counts.get((chat_id, message_id), {k: 0 for k in REACTIONS})
    buttons = [
        types.InlineKeyboardButton(
            f"{REACTIONS[key]} {counts[key]}" if counts[key] > 0 else REACTIONS[key],
            callback_data=f"{key}:{chat_id}:{message_id}"
        ) for key in REACTIONS
    ]
    markup.add(*buttons)
    markup.add(types.InlineKeyboardButton("📤 Share", switch_inline_query=latest_quotes.get(chat_id, "")))
    return markup

# Admin Check
def is_admin(chat_id, user_id):
    try:
        member = bot.get_chat_member(chat_id, user_id)
        return member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Admin check error: {e}")
        return False

# Start Command
@bot.message_handler(commands=['start'])
def start_command(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /start command in chat {chat_id}")
    bot_username = f"@{bot.get_me().username}"
    caption = (
        f"🌟 <b>Welcome to {bot_username}!</b> 🌟\n\n"
        "<i>✨ Your ultimate group companion for inspiration and fun!</i>\n"
        "══════════════════════\n"
        "💬 <b>Explore daily Hindi quotes</b> to spark motivation\n"
        "🏆 <b>Compete on leaderboards</b> with group activity\n"
        "📊 <b>Track your stats</b> and group rankings\n"
        "⚙️ <b>Customize settings</b> for quotes and welcomes\n"
        "══════════════════════\n"
        "🔥 Type <b>/help</b> to unlock all commands!\n"
        "<tg-spoiler>💡 Share quotes, vote in polls, and more!</tg-spoiler>"
    )
    inline_keyboard = [
        [
            types.InlineKeyboardButton("📖 Commands", callback_data="/help"),
            types.InlineKeyboardButton("🤝 Support", url="https://t.me/hiden_25")
        ],
        [
            types.InlineKeyboardButton(
                "➕ Add to Group",
                url=f"https://t.me/{bot.get_me().username}?startgroup=true"
            )
        ]
    ]
    markup = types.InlineKeyboardMarkup(inline_keyboard)
    random_photo = random.choice(photos)

    try:
        if chat_id in start_message_ids:
            bot.edit_message_media(
                chat_id=chat_id,
                message_id=start_message_ids[chat_id],
                media=types.InputMediaPhoto(
                    media=random_photo,
                    caption=caption,
                    parse_mode="HTML"
                ),
                reply_markup=markup
            )
        else:
            msg = bot.send_photo(
                chat_id=chat_id,
                photo=random_photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup
            )
            start_message_ids[chat_id] = msg.message_id
        logger.info(f"/start command processed successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Start command error in chat {chat_id}: {e}")
        try:
            msg = bot.send_photo(
                chat_id=chat_id,
                photo=random_photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup
            )
            start_message_ids[chat_id] = msg.message_id
        except Exception as e:
            logger.error(f"Start send error in chat {chat_id}: {e}")
            bot.reply_to(message, "⚠️ Error sending welcome message.")

# Help Command
@bot.message_handler(commands=['help'])
def help_command(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /help command in chat {chat_id}")
    caption = (
        "📜 <b>Command Guide for Your Bot</b> 📜\n\n"
        "<i>🔥 Use these commands to explore all features!</i>\n"
        "══════════════════════\n"
        "🌟 <b>/start</b> - Launch the bot with a warm welcome\n"
        "📖 <b>/help</b> - Display this command guide\n"
        "🧠 <b>/quotes</b> - Get an inspiring Hindi quote\n"
        "📊 <b>/totalquotes</b> - View quote stats for groups\n"
        "🏆 <b>/leaderboard</b> - Check group message rankings\n"
        "📈 <b>/mystats</b> - See your personal message stats\n"
        "👤 <b>/profile</b> - View your profile and activity\n"
        "⚙️ <b>/settype [text/img]</b> - Set quote format (Admin)\n"
        "⏰ <b>/setquotetime</b> - Schedule quotes (Admin)\n"
        "👋 <b>/setwelcome &lt;message&gt;</b> - Set welcome message (Admin)\n"
        "🗳️ <b>/pollquote</b> - Create a quote comparison poll\n"
        "🔄 <b>/reboot</b> - Restart the bot (Owner only)\n"
        "══════════════════════\n"
        "<i>© @hiden_25 | Enjoy the experience!</i>"
    )
    inline_keyboard = [
        [
            types.InlineKeyboardButton("📜 Quotes", callback_data="/quotes"),
            types.InlineKeyboardButton("🔙 Back", callback_data="/start")
        ],
        [
            types.InlineKeyboardButton("❌ Close", callback_data="/close")
        ]
    ]
    markup = types.InlineKeyboardMarkup(inline_keyboard)
    random_photo = random.choice(photos)

    try:
        if chat_id in help_message_ids:
            bot.edit_message_media(
                chat_id=chat_id,
                message_id=help_message_ids[chat_id],
                media=types.InputMediaPhoto(
                    media=random_photo,
                    caption=caption,
                    parse_mode="HTML"
                ),
                reply_markup=markup
            )
        else:
            msg = bot.send_photo(
                chat_id=chat_id,
                photo=random_photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup
            )
            help_message_ids[chat_id] = msg.message_id
        logger.info(f"/help command processed successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Help command error in chat {chat_id}: {e}")
        try:
            msg = bot.send_photo(
                chat_id=chat_id,
                photo=random_photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup
            )
            help_message_ids[chat_id] = msg.message_id
        except Exception as e:
            logger.error(f"Help send error in chat {chat_id}: {e}")
            bot.reply_to(message, "⚠️ Error sending help menu.")

# Callback Handlers
@bot.callback_query_handler(func=lambda call: call.data in ["/quotes", "/close", "/start", "/help"])
def callback_handler(call):
    chat_id = getattr(call.message.chat, 'id', None)
    logger.info(f"Processing callback {call.data} in chat {chat_id}")
    try:
        if call.data == "/quotes":
            bot.answer_callback_query(call.id)
            generate_quote(call.message)
        elif call.data == "/close":
            bot.delete_message(chat_id, call.message.message_id)
            if chat_id in help_message_ids:
                del help_message_ids[chat_id]
            bot.answer_callback_query(call.id)
        elif call.data == "/start":
            bot.answer_callback_query(call.id)
            start_command(call.message)
        elif call.data == "/help":
            bot.answer_callback_query(call.id)
            help_command(call.message)
        logger.info(f"Callback {call.data} processed successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Callback error in chat {chat_id}: {e}")
        bot.answer_callback_query(call.id, "⚠️ Error processing request.")

# Reaction Callback Handler
@bot.callback_query_handler(func=lambda call: call.data.split(':')[0] in REACTIONS)
def handle_reaction(call):
    chat_id = getattr(call.message.chat, 'id', None)
    logger.info(f"Processing reaction callback in chat {chat_id}")
    try:
        reaction, chat_id_str, message_id_str = call.data.split(':')
        chat_id = int(chat_id_str)
        message_id = int(message_id_str)
        user_id = call.from_user.id
        key = (chat_id, message_id)
        if key not in reaction_counts:
            reaction_counts[key] = {k: 0 for k in REACTIONS}
        if key not in user_reactions:
            user_reactions[key] = {}
        if user_id in user_reactions[key]:
            old_reaction = user_reactions[key][user_id]
            reaction_counts[key][old_reaction] -= 1
        user_reactions[key][user_id] = reaction
        reaction_counts[key][reaction] += 1
        markup = create_reaction_buttons(chat_id, message_id)
        bot.edit_message_reply_markup(chat_id, message_id, reply_markup=markup)
        bot.answer_callback_query(call.id)
        user_stats.setdefault(user_id, {}).setdefault(chat_id, {
            'join_date': datetime.datetime.now().isoformat(),
            'reactions_given': 0,
            'quote_requests': 0
        })['reactions_given'] += 1
        save_data()
        logger.info(f"Reaction processed successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Reaction error in chat {chat_id}: {e}")
        bot.answer_callback_query(call.id, "⚠️ Error processing reaction.")

# Set Type Command
@bot.message_handler(commands=['settype'])
def set_type(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /settype command in chat {chat_id}")
    if not is_admin(chat_id, message.from_user.id):
        bot.reply_to(message, "❌ आपको यह कमांड इस्तेमाल करने के लिए एडमिन होना चाहिए।")
        return
    args = message.text.split()
    if len(args) != 2 or args[1].lower() not in ['text', 'img']:
        bot.reply_to(message, "❌ सही उपयोग:\n/settype text\nया\n/settype img")
        return
    chat_settings[chat_id] = args[1].lower()
    save_data()
    bot.reply_to(message, f"✅ सेटिंग सेव हो गई है: कोट्स अब <b>{args[1].lower()}</b> के रूप में भेजे जाएंगे।", parse_mode="HTML")
    logger.info(f"Set type to {args[1].lower()} in chat {chat_id}")

# Set Quote Time Command
@bot.message_handler(commands=['setquotetime'])
def set_quote_time(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /setquotetime command in chat {chat_id}")
    if not is_admin(chat_id, message.from_user.id):
        bot.reply_to(message, "❌ आपको यह कमांड इस्तेमाल करने के लिए एडमिन होना चाहिए।")
        return
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("⏰ Hours", callback_data=f"interval:hours:{chat_id}"),
        types.InlineKeyboardButton("⏱️ Minutes", callback_data=f"interval:minutes:{chat_id}")
    )
    bot.reply_to(message, "📅 कोट्स भेजने का अंतराल चुनें:", reply_markup=markup)
    logger.info(f"Prompted interval selection in chat {chat_id}")

# Callback Query Handler for Interval Selection
@bot.callback_query_handler(func=lambda call: call.data.startswith('interval:'))
def handle_interval_selection(call):
    chat_id = getattr(call.message.chat, 'id', None)
    logger.info(f"Processing interval selection callback in chat {chat_id}")
    try:
        _, unit, chat_id_str = call.data.split(':')
        chat_id = int(chat_id_str)
        markup = types.InlineKeyboardMarkup()
        
        if unit == "minutes":
            intervals = [10, 20, 30, 40, 50, 60]
            buttons = [
                types.InlineKeyboardButton(f"{i} मिनट", callback_data=f"setinterval:{i*60}:{chat_id}")
                for i in intervals
            ]
            markup.add(*buttons)
            bot.edit_message_text(
                "⏱️ मिनटों में अंतराल चुनें:", 
                chat_id=chat_id, 
                message_id=call.message.message_id, 
                reply_markup=markup
            )
        elif unit == "hours":
            intervals = [1, 3, 5, 6]
            buttons = [
                types.InlineKeyboardButton(f"{i} घंटे", callback_data=f"setinterval:{i*3600}:{chat_id}")
                for i in intervals
            ]
            markup.add(*buttons)
            bot.edit_message_text(
                "⏰ घंटों में अंतराल चुनें:", 
                chat_id=chat_id, 
                message_id=call.message.message_id, 
                reply_markup=markup
            )
        bot.answer_callback_query(call.id)
        logger.info(f"Interval selection processed: {unit} in chat {chat_id}")
    except Exception as e:
        logger.error(f"Interval selection error in chat {chat_id}: {e}")
        bot.answer_callback_query(call.id, "⚠️ कुछ गलत हो गया।")

# Callback Query Handler for Setting Interval
@bot.callback_query_handler(func=lambda call: call.data.startswith('setinterval:'))
def handle_set_interval(call):
    chat_id = getattr(call.message.chat, 'id', None)
    logger.info(f"Processing set interval callback in chat {chat_id}")
    try:
        _, interval_str, chat_id_str = call.data.split(':')
        chat_id = int(chat_id_str)
        interval = int(interval_str)
        chat_schedules[chat_id] = interval
        last_quote_times[chat_id] = 0
        unit = "मिनट" if interval <= 3600 else "घंटे"
        value = interval // 60 if unit == "मिनट" else interval // 3600
        bot.edit_message_text(
            f"✅ कोट्स अब हर <b>{value} {unit}</b> में भेजे जाएंगे।",
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=None,
            parse_mode="HTML"
        )
        bot.answer_callback_query(call.id)
        logger.info(f"Set interval to {value} {unit} in chat {chat_id}")
    except Exception as e:
        logger.error(f"Set interval error in chat {chat_id}: {e}")
        bot.answer_callback_query(call.id, "⚠️ कुछ गलत हो गया।")

# Set Welcome Command
@bot.message_handler(commands=['setwelcome'])
def set_welcome(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /setwelcome command in chat {chat_id}")
    if not is_admin(chat_id, message.from_user.id):
        bot.reply_to(message, "❌ Only admins can use this command.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(message, "❓ Usage: /setwelcome <message>")
        return
    welcome_messages[chat_id] = args[1]
    bot.reply_to(message, "✅ Welcome message set.")
    logger.info(f"Welcome message set for chat {chat_id}")

# My Stats Command
@bot.message_handler(commands=['mystats'])
def my_stats(message):
    chat_id = getattr(message.chat, 'id', None)
    user_id = message.from_user.id
    logger.info(f"Processing /mystats command in chat {chat_id}")
    if chat_id not in leaderboard_data or user_id not in leaderboard_data.get(chat_id, {}):
        bot.reply_to(message, "📊 You haven't sent any messages yet!")
        return
    user_data = leaderboard_data[chat_id][user_id]
    today_key = time.strftime("%Y-%m-%d")
    week_key = time.strftime("%Y-W%U")
    caption = f"📊 <b>Your Stats in {message.chat.title}</b>\n\n"
    caption += f"📅 Daily Messages: {user_data['daily'].get(today_key, 0)}\n"
    caption += f"📈 Weekly Messages: {user_data['weekly'].get(week_key, 0)}\n"
    caption += f"🏆 Total Messages: {user_data['overall']}\n"
    bot.reply_to(message, caption, parse_mode="HTML")
    logger.info(f"/mystats processed successfully in chat {chat_id}")

# Profile Command
@bot.message_handler(commands=['profile'])
def profile_command(message):
    chat_id = getattr(message.chat, 'id', None)
    user_id = message.from_user.id
    logger.info(f"Processing /profile command in chat {chat_id}")
    stats = user_stats.get(user_id, {}).get(chat_id, {
        'join_date': 'N/A',
        'reactions_given': 0,
        'quote_requests': 0
    })
    caption = (
        f"📋 <b>Profile Stats for {message.from_user.first_name}</b>\n\n"
        f"📅 Joined: {stats['join_date']}\n"
        f"👍 Reactions Given: {stats['reactions_given']}\n"
        f"🧠 Quotes Requested: {stats['quote_requests']}\n"
        f"🏆 Total Messages: {leaderboard_data.get(chat_id, {}).get(user_id, {}).get('overall', 0)}"
    )
    bot.reply_to(message, caption, parse_mode="HTML")
    logger.info(f"/profile processed successfully in chat {chat_id}")

# Quotes Command
@bot.message_handler(commands=['quotes'])
def generate_quote(message):
    global total_quote_count
    chat_id = message.chat.id
    user_id = message.from_user.id
    logger.info(f"Processing /quotes command in chat {chat_id}")

    # Fetch quote
    try:
        if main_loop is None:
            logger.error("Main event loop not initialized")
            bot.reply_to(message, "⚠️ Bot initialization error. Please try again later.")
            return
        future = asyncio.run_coroutine_threadsafe(get_hindi_quote(), main_loop)
        quote = future.result(timeout=10)  # 10-second timeout
        if not quote:
            bot.reply_to(message, "⚠️ कोट प्राप्त करने में त्रुटि। कृपया बाद में पुनः प्रयास करें।")
            logger.error("Failed to fetch quote")
            return
    except Exception as e:
        logger.error(f"Quote fetch error in chat {chat_id}: {e}")
        bot.reply_to(message, "⚠️ कोट प्राप्त करने में त्रुटि। कृपया बाद में पुनः प्रयास करें।")
        return

    try:
        if message.chat.type in ['group', 'supergroup']:
            send_type = chat_settings.get(chat_id, 'text')
            if send_type == 'text':
                msg = bot.send_message(chat_id, f"🧠💖 कोट:\n\n{quote}", parse_mode="HTML")
                markup = create_reaction_buttons(chat_id, msg.message_id)
                bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
            else:
                img = generate_quote_image(quote)
                if img:
                    msg = bot.send_photo(chat_id, img, caption="🧠 कोट")
                    markup = create_reaction_buttons(chat_id, msg.message_id)
                    bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
                else:
                    msg = bot.send_message(chat_id, f"🧠 कोट:\n\n{quote}", parse_mode="HTML")
                    markup = create_reaction_buttons(chat_id, msg.message_id)
                    bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
            with total_quote_count_lock:
                quote_counts[chat_id] = quote_counts.get(chat_id, 0) + 1
                total_quote_count += 1
            user_stats.setdefault(user_id, {}).setdefault(chat_id, {
                'join_date': datetime.datetime.now().isoformat(),
                'reactions_given': 0,
                'quote_requests': 0
            })['quote_requests'] += 1
            save_data()
        else:
            msg = bot.send_message(chat_id, f"🧠💖 कोट:\n\n{quote}", parse_mode="HTML")
            markup = create_reaction_buttons(chat_id, msg.message_id)
            bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
            user_stats.setdefault(user_id, {}).setdefault(chat_id, {
                'join_date': datetime.datetime.now().isoformat(),
                'reactions_given': 0,
                'quote_requests': 0
            })['quote_requests'] += 1
            save_data()
        latest_quotes[chat_id] = quote
        logger.info(f"Quote sent successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Quote send error in chat {chat_id}: {e}")
        bot.reply_to(message, "⚠️ कोट भेजने में त्रुटि।")

# Total Quotes Command
@bot.message_handler(commands=['totalquotes'])
def total_quotes(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /totalquotes command in chat {chat_id}")
    try:
        if not subscribed_chats:
            bot.reply_to(message, "📊 अभी तक कोई कोट नहीं भेजा गया या कोई ग्रुप शामिल नहीं है।")
            logger.info("No subscribed chats for /totalquotes")
            return
        caption = f"📊 <b>Total Quotes Sent: {total_quote_count}</b>\n\n"
        group_stats = []
        for group_id in subscribed_chats:
            try:
                group_info = bot.get_chat(group_id)
                group_name = group_info.title or "Unknown Group"
                count = quote_counts.get(group_id, 0)
                group_stats.append((group_name, count, group_id))
            except Exception as e:
                logger.error(f"Group name fetch error for group {group_id}: {e}")
                continue
        group_stats.sort(key=lambda x: x[1], reverse=True)
        if message.chat.type in ['group', 'supergroup']:
            current_count = quote_counts.get(chat_id, 0)
            caption += f"📖 <b>This Group</b>: {current_count} quotes\n\n"
        caption += "🏆 <b>Group Rankings by Quotes</b>:\n\n"
        if not group_stats:
            caption += "<i>No quotes recorded yet!</i>"
        else:
            for i, (name, count, _) in enumerate(group_stats, 1):
                rank = ""
                if i == 1:
                    rank = "🏆 1st"
                elif i == 2:
                    rank = "🥈 2nd"
                elif i == 3:
                    rank = "🥉 3rd"
                caption += f"{i}. {name} — {count} quotes {rank}\n"
        caption += f"\n🛬 <b>Total Groups</b>: {len(subscribed_chats)}"
        bot.reply_to(message, caption, parse_mode="HTML")
        logger.info(f"/totalquotes processed successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Total quotes error in chat {chat_id}: {e}")
        bot.reply_to(message, "⚠️ कोट आंकड़े प्राप्त करने में त्रुटि। कृपया बाद में पुनः प्रयास करें।")

# Poll Quote Command
@bot.message_handler(commands=['pollquote'])
def poll_quote(message):
    chat_id = message.chat.id
    logger.info(f"Processing /pollquote command in chat {chat_id}")
    try:
        if main_loop is None:
            logger.error("Main event loop not initialized")
            bot.reply_to(message, "⚠️ Bot initialization error. Please try again later.")
            return
        future1 = asyncio.run_coroutine_threadsafe(get_hindi_quote(), main_loop)
        future2 = asyncio.run_coroutine_threadsafe(get_hindi_quote(), main_loop)
        quote1 = future1.result(timeout=10)
        quote2 = future2.result(timeout=10)
        if not quote1 or not quote2:
            bot.reply_to(message, "⚠️ कोट प्राप्त करने में त्रुटि। कृपया बाद में पुनः प्रयास करें।")
            logger.error("Failed to fetch quotes for poll")
            return
        poll = bot.send_poll(
            chat_id=chat_id,
            question="Which quote do you like better? 🧠",
            options=[quote1.split('\n\n— ')[0][:100], quote2.split('\n\n— ')[0][:100]],  # Truncate for poll
            is_anonymous=False,
            allows_multiple_answers=False
        )
        logger.info(f"Poll created successfully in chat {chat_id}")
    except Exception as e:
        logger.error(f"Poll quote error in chat {chat_id}: {e}")
        bot.reply_to(message, "⚠️ Error creating quote poll.")

# Quote Scheduler
async def send_quote_to_all():
    global total_quote_count
    logger.info("Running quote scheduler...")
    quote = await get_hindi_quote()
    if not quote:
        logger.error("Quote fetch failed in scheduler")
        return
    current_time = time.time()
    for chat_id in list(subscribed_chats):
        interval = chat_schedules.get(chat_id, 24*3600)
        last_sent = last_quote_times.get(chat_id, 0)
        if current_time - last_sent < interval:
            continue
        try:
            send_type = chat_settings.get(chat_id, 'text')
            if send_type == 'text':
                msg = bot.send_message(chat_id, f"🧠 <b>आज का विचार</b>:\n\n{quote}", parse_mode="HTML")
                markup = create_reaction_buttons(chat_id, msg.message_id)
                bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
            else:
                img = generate_quote_image(quote)
                if img:
                    msg = bot.send_photo(chat_id, img, caption="🧠 आज का विचार")
                    markup = create_reaction_buttons(chat_id, msg.message_id)
                    bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
                else:
                    msg = bot.send_message(chat_id, f"🧠 <b>आज का विचार</b>:\n\n{quote}", parse_mode="HTML")
                    markup = create_reaction_buttons(chat_id, msg.message_id)
                    bot.edit_message_reply_markup(chat_id, msg.message_id, reply_markup=markup)
            last_quote_times[chat_id] = current_time
            latest_quotes[chat_id] = quote
            with total_quote_count_lock:
                quote_counts[chat_id] = quote_counts.get(chat_id, 0) + 1
                total_quote_count += 1
            save_data()
            logger.info(f"Scheduled quote sent to chat {chat_id}")
        except Exception as e:
            logger.error(f"Send error in chat {chat_id}: {e}")

async def scheduler():
    logger.info("Starting scheduler...")
    while True:
        await send_quote_to_all()
        await asyncio.sleep(60)

# Leaderboard Logic
def update_leaderboard(chat_id, user_id, user_name):
    if chat_id not in leaderboard_data:
        leaderboard_data[chat_id] = {}
    if user_id not in leaderboard_data[chat_id]:
        leaderboard_data[chat_id][user_id] = {
            "name": user_name,
            "daily": {},
            "weekly": {},
            "overall": 0
        }
    
    user_data = leaderboard_data[chat_id][user_id]
    user_data["name"] = user_name
    user_data["overall"] += 1
    
    today_key = time.strftime("%Y-%m-%d")
    week_key = time.strftime("%Y-W%U")
    
    user_data["daily"][today_key] = user_data["daily"].get(today_key, 0) + 1
    user_data["weekly"][week_key] = user_data["weekly"].get(week_key, 0) + 1
    save_data()

def clean_leaderboard_data():
    logger.info("Starting leaderboard cleanup...")
    while True:
        current_date = time.strftime("%Y-%m-%d")
        current_week = time.strftime("%Y-W%U")
        for chat_id in leaderboard_data:
            for user_id in leaderboard_data[chat_id]:
                data = leaderboard_data[chat_id][user_id]
                data["daily"] = {k: v for k, v in data["daily"].items() if k >= current_date}
                data["weekly"] = {k: v for k, v in data["weekly"].items() if k >= current_week}
        save_data()
        logger.info("Leaderboard cleanup completed")
        time.sleep(24*3600)

async def display_leaderboard(chat_id, mode="overall"):
    logger.info(f"Displaying leaderboard for chat {chat_id}, mode: {mode}")
    if chat_id not in leaderboard_data or not leaderboard_data[chat_id]:
        return "📊 <b>लीडरबोर्ड</b>\n\n<i>कोई संदेश अभी तक नहीं!</i>", None

    leaderboard = []
    today_key = time.strftime("%Y-%m-%d")
    week_key = time.strftime("%Y-W%U")
    
    if mode == "daily":
        for user_id, data in leaderboard_data[chat_id].items():
            count = data["daily"].get(today_key, 0)
            if count > 0:
                leaderboard.append((data["name"], count))
        caption = "📅 <b>दैनिक लीडरबोर्ड</b>\n\n"
    elif mode == "weekly":
        for user_id, data in leaderboard_data[chat_id].items():
            count = data["weekly"].get(week_key, 0)
            if count > 0:
                leaderboard.append((data["name"], count))
        caption = "📈 <b>साप्ताहिक लीडरबोर्ड</b>\n\n"
    else:
        for user_id, data in leaderboard_data[chat_id].items():
            count = data["overall"]
            if count > 0:
                leaderboard.append((data["name"], count))
        caption = "🏆 <b>कुल लीडरबोर्ड</b>\n\n"

    leaderboard.sort(key=lambda x: x[1], reverse=True)
    for i, (name, count) in enumerate(leaderboard[:10], 1):
        rank = "🏅" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
        caption += f"{i}. {name} — {count} संदेश {rank}\n"
    
    if not leaderboard:
        caption += "<i>कोई संदेश अभी तक नहीं!</i>"
    
    caption += f"\n🧮 <b>कुल उपयोगकर्ता</b>: {len(leaderboard_data.get(chat_id, {}))}"
    logger.info(f"Leaderboard generated for chat {chat_id}")
    return caption, InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 दैनिक", callback_data=f"leaderboard:daily:{chat_id}"),
            InlineKeyboardButton("📈 साप्ताहिक", callback_data=f"leaderboard:weekly:{chat_id}"),
            InlineKeyboardButton("🏆 कुल", callback_data=f"leaderboard:overall:{chat_id}")
        ]
    ])

# Pyrogram Events
@app.on_message(filters.new_chat_members)
async def welcome_handler(client, message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"New chat members in chat {chat_id}")
    for member in message.new_chat_members:
        name = member.first_name or member.mention
        try:
            await message.reply_text(f"👋 <b>{name}</b>", parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending welcome message in chat {chat_id}: {e}")

@app.on_message(filters.text & filters.group & ~filters.service)
async def update_leaderboard(chat_id, user_id):
    quote = json.dumps({
        'chat_id': chat_id,
        'user_id': user_id
    })
    logger.info(f"Message counted in chat {chat_id}")

@app.on_message(filters.command("leaderboard") & filters.group)
async def leaderboard_command(client, message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /leaderboard in chat {chat_id}")
    try:
        caption, markup = await display_leaderboard(message.chat.id, "overall")
        await message.reply_text(caption, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Leaderboard command error in {chat_id}: {e}")
        await message.reply_text(f"Error: {e}")

@app.on_message(filters.command("reboot") & filters.user(BOT_OWNER_ID))
async def reboot_handler(client, message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing /reboot command in {chat_id}")
    try:
        await message.reply_text("🔄 Rebooting...")
        await client.stop()
        logger.info("Shutting down for reboot")
        os.execv(sys.executable, ['python3'] + sys.argv[1:])
    except Exception as e:
        logger.error(f"Reboot error: {e}")
        await message.reply_text(f"⚠️ Reboot failed: {e}")

@app.on_callback_query(filters.regex(r'^leaderboard:'))
async def leaderboard_callback(client, callback_query):
    chat_id = getattr(callback_query.message.chat, 'id', None)
    logger.info(f"Processing leaderboard callback in {chat_id}")
    try:
        _, mode, chat_id_str = callback_query.data.split(':')
        chat_id = int(chat_id_str)
        caption, markup = await display_leaderboard(chat_id, mode)
        await callback_query.message.edit_text(caption, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer()
        logger.info(f"Leaderboard callback processed successfully in {chat_id}")
    except Exception as e:
        logger.error(f"Leaderboard callback error in {chat_id}: {e}")
        await callback_query.answer(f"Error: {e}")

# Join Handler
@bot.message_handler(content_types=['new_chat_members'])
def new_member_handler(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing new member event in chat {chat_id}")
    try:
        for member in message.new_chat_members:
            chat_id = message.chat.id
            if member.id == bot.get_me().id:
                subscribed_chats.add(chat_id)
                if chat_id not in chat_settings:
                    chat_settings[chat_id] = 'text'
                save_data()
                bot.send_message(chat_id, "<b>धन्यवाद!</b> मैं इस ग्रुप में जुड़ गया हूँ और अब से दैनिक कोट्स भेजूंगा।", parse_mode="HTML")
                logger.info(f"Bot joined chat {chat_id}")
            else:
                # Create a user mention
                if member.username:
                    user_mention = f"@{member.username}"
                else:
                    # Escape special characters in first_name
                    first_name = (member.first_name or "User").replace('&', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    user_mention = f'<a href="tg://user?id={member.id}">{first_name}</a>'
                # Get custom welcome message or default, escape special characters
                welcome_msg = welcome_messages.get(chat_id, "Welcome to the group!").replace('&', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                # Combine mention and welcome message
                full_message = f"{user_mention} {welcome_msg}"
                bot.send_message(chat_id, full_message, parse_mode="HTML")
                logger.info(f"Welcomed user {member.id} in chat {chat_id}")
                if member.id != bot.get_me().id:
                    user_stats.setdefault(member.id, {}).setdefault(chat_id, {
                        'join_date': datetime.datetime.now().isoformat(),
                        'reactions_given': 0,
                        'quote_requests': 0
                    })
                    save_data()
    except Exception as e:
        logger.error(f"New member handler error in chat {chat_id}: {e}")
        # Fallback: send plain text without mention if HTML fails
        try:
            welcome_msg = welcome_messages.get(chat_id, "Welcome to the group!")
            bot.send_message(chat_id, f"Welcome {member.first_name or 'User'}! {welcome_msg}")
            logger.info(f"Fallback welcome sent for user {member.id} in chat {chat_id}")
        except Exception as e2:
            logger.error(f"Fallback welcome error in chat {chat_id}: {e2}")

# Leave Handler
@bot.message_handler(content_types=['left_chat_member'])
def left_member_handler(message):
    chat_id = getattr(message.chat, 'id', None)
    logger.info(f"Processing left member event in chat {chat_id}")
    try:
        if message.left_chat_member.id == bot.get_me().id:
            subscribed_chats.discard(chat_id)
            for data in [leaderboard_data, latest_quotes, quote_counts, start_message_ids, help_message_ids]:
                if chat_id in data:
                    del data[chat_id]
            save_data()
            logger.info(f"Bot left chat {chat_id}")
    except Exception as e:
        logger.error(f"Left member handler error in chat {chat_id}: {e}")

# Start Bot
async def main():
    global main_loop
    logger.info("Starting bot...")
    try:
        load_data()
        main_loop = asyncio.get_event_loop()
        logger.info("Main event loop initialized")
        await app.start()
        logger.info("Pyrogram client started")
        loop = asyncio.get_event_loop()
        loop.create_task(scheduler())
        threading.Thread(target=clean_leaderboard_data, daemon=True).start()
        
        def run_polling():
            try:
                bot.infinity_polling()
                logger.info("TeleBot polling started")
            except Exception as e:
                logger.error(f"Polling error: {e}")
        
        polling_thread = threading.Thread(target=run_polling, daemon=True)
        polling_thread.start()
        
        logger.info("Bot is fully running")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            await app.stop()
            bot.stop_polling()
            logger.info("Bot stopped")
    except Exception as e:
        logger.error(f"Main loop error: {e}")

if __name__ == "__main__":
    asyncio.run(main())