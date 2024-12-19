import pytz
import logging
import inspect
import sys
import asyncio
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta, timezone
from shared_database import SharedDatabase
from telegram import Update
from telegram.ext import (
    ChatMemberHandler,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    CallbackContext,
    Application,
)

from config import (
    BOT_TOKEN
)


# Configure logging
when = 'midnight'  # Rotate logs at midnight (other options include 'H', 'D', 'W0' - 'W6', 'MIDNIGHT', or a custom time)
interval = 1  # Rotate daily
backup_count = 7  # Retain logs for 7 days
log_handler = TimedRotatingFileHandler('app.log', when=when, interval=interval, backupCount=backup_count)
log_handler.suffix = "%Y-%m-%d"  # Suffix for log files (e.g., 'my_log.log.2023-10-22')

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        log_handler,
    ]
)

# Create a separate handler for console output with a higher level (WARNING)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # Set the level to WARNING or higher
console_formatter = logging.Formatter("LOUNGE HUB: %(message)s")
console_handler.setFormatter(console_formatter)

# Attach the console handler to the root logger
logging.getLogger().addHandler(console_handler)


# Global variables
loungehubbot = None
app = None
db = SharedDatabase()
utc_timezone = pytz.utc


########## ERROR HANDLING ##########
def handle_error(exception: Exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    current_frame = inspect.currentframe()
    caller_frame = current_frame.f_back if current_frame else None
    function_name = caller_frame.f_code.co_name if caller_frame else 'Unknown'
    
    logging.warning(
        f"Error in function '{function_name}' - {exception}. "
        f"Exception Raised In: {exc_traceback.tb_frame.f_code.co_name} - "
        f"Line: {exc_traceback.tb_lineno} - "
        f"Type: {exc_type}. "

    )
    return


async def start_command(update: Update, context: CallbackContext) -> None:
    try:
        logging.info("Received /start command")
        reply_text = "Welcome to the Chat Lounge Hub! Use /help to see available commands.\n"
        reply_text += "\nActive Lounge Bots:\n"
        lounges = db.get_active_lounges()
        # Loop through the lounges list and create reply text
        for lounge in lounges:
            last_updated_dt = datetime.strptime(lounge['last_updated'], '%Y-%m-%d %H:%M:%S.%f')
            formatted_date = last_updated_dt.strftime('%Y-%m-%d %H:%M:%S')
            # formatted_date = datetime.fromtimestamp(lounge['last_updated'], tz=utc_timezone).strftime('%Y-%m-%d %H:%M:%S')
            reply_text += f"@{lounge['name']} - {lounge['active_user_count']} Active Users - Last Update: {formatted_date}\n"
        reply_text += """
To join a lounge, click the @username link and start the bot.

You can be in only one bot at a time. If you join another bot, you will need to leave your current bot

You'll need to send a few videos to register and start seeing messages from others. The bot will tell you how many.
"""
        await update.message.reply_text(reply_text)
    except Exception as e:
        handle_error(e)
    return


async def help_command(update: Update, context: CallbackContext) -> None:
    try:
        logging.info("Received /help command")
        await update.message.reply_text("Available commands:\n"
                                  "/start - Start the bot\n"
                                  "/help - Display this help message")
    except Exception as e:
        handle_error(e)
    return


# set a job in the bot job queue to run the db.set_inactive_lounges() function every 5 minutes
async def create_set_inactive_job_on_startup() -> None:
    global app
    try:
        logging.info("Checking for inactive lounges...")
        app.job_queue.run_repeating(
            db.timed_updates, interval=60, first=0
        )

    except Exception as e:
        handle_error(e)
    return

async def post_init(application: Application):
    asyncio.create_task(create_set_inactive_job_on_startup())


def main() -> None:
    global loungehubbot
    global app

    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))

    try:
        loungehubbot = application.bot
        app = application
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()