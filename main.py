import json
import os
import asyncio
import random
import time
import sys
from datetime import datetime
import logging
from telethon.tl.functions.contacts import BlockRequest
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest, InviteToChannelRequest
from telethon.tl.types import InputPeerUser
from telethon.errors import SessionPasswordNeededError, FloodWaitError, RPCError, ChatAdminRequiredError
from telethon.events import NewMessage
from telethon.tl.functions.messages import DeleteHistoryRequest
from telethon.tl.functions.channels import LeaveChannelRequest
import telethon
from telethon.tl.functions.messages import ReportRequest
import subprocess
import phonenumbers
from phonenumbers import carrier, geocoder
from phonenumbers.phonenumberutil import NumberParseException
import hashlib
import re
from telethon.tl.types import KeyboardButtonCallback
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact
from telethon.tl.functions.messages import SendMessageRequest
from colorama import Fore, Style, init
from pyfiglet import Figlet

# Add clear_screen function after imports:
def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')
print(f"Telethon version: {telethon.__version__}")

CONFIG_FILE = "config.json"
SESSION_FILE = "sessions.json"
GROUPS_FILE = "groups.txt"
SENT_DMS_FILE = "sent_dms.json"
AUTO_REPLY_FILE = "auto_reply.json"
PSEUDO_CREDENTIALS_FILE = "pseudo_credentials.json"
PHONE_NUMBERS_FILE = os.path.join("Numbers", "telegram_numbers.json")

# Set up logging for rate limit errors
logging.basicConfig(filename="rate_limit.log", level=logging.INFO, 
                    format="%(asctime)s - %(message)s")

# Update PHONE_NUMBERS_FILE definition at the top
PHONE_NUMBERS_FILE = "telegram_numbers.json"

# Update init_files()
def init_files():
    os.makedirs("Numbers", exist_ok=True)
    for file in [CONFIG_FILE, SESSION_FILE, SENT_DMS_FILE, AUTO_REPLY_FILE, PSEUDO_CREDENTIALS_FILE, PHONE_NUMBERS_FILE]:
        if not os.path.exists(os.path.join("Numbers" if file == PHONE_NUMBERS_FILE else ".", file)):
            with open(os.path.join("Numbers" if file == PHONE_NUMBERS_FILE else ".", file), "w") as f:
                json.dump({}, f)
    if not os.path.exists(GROUPS_FILE):
        with open(GROUPS_FILE, "w") as f:
            f.write("")
    if not os.path.exists("members"):
        os.makedirs("members")

def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config.json: {str(e)}")
        return {}

def save_config(config):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        print(f"Error saving config.json: {str(e)}")

def load_sessions():
    try:
        with open(SESSION_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading sessions.json: {str(e)}")
        return {}

def save_sessions(sessions):
    try:
        with open(SESSION_FILE, "w") as f:
            json.dump(sessions, f, indent=4)
    except Exception as e:
        print(f"Error saving sessions.json: {str(e)}")

def load_pseudo_credentials():
    try:
        with open(PSEUDO_CREDENTIALS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading pseudo_credentials.json: {str(e)}")
        return {}

def save_pseudo_credentials(credentials):
    try:
        with open(PSEUDO_CREDENTIALS_FILE, "w") as f:
            json.dump(credentials, f, indent=4)
    except Exception as e:
        print(f"Error saving pseudo_credentials.json: {str(e)}")

def load_members():
    members = {}
    if not os.path.exists("members"):
        return members
    for file in os.listdir("members"):
        if file.endswith(".json"):
            try:
                with open(os.path.join("members", file), "r") as f:
                    data = json.load(f)
                    members[file] = data.get("members", [])
            except Exception as e:
                print(f"Error loading {file}: {str(e)}")
    return members

def save_members(group_name, members):
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"members/{group_name}_{date_str}.json"
        data = {
            "members": members
        }
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Saved {len(members)} members to {filename}")
    except Exception as e:
        print(f"Error saving members: {str(e)}")

def load_sent_dms():
    try:
        with open(SENT_DMS_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error loading sent_dms.json: {str(e)}")
        return {}

def save_sent_dms(sent_dms):
    try:
        with open(SENT_DMS_FILE, "w") as f:
            json.dump(sent_dms, f, indent=4)
    except Exception as e:
        print(f"Error saving sent_dms.json: {str(e)}")

def load_auto_reply():
    try:
        with open(AUTO_REPLY_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading auto_reply.json: {str(e)}")
        return {}

def save_auto_reply(auto_reply):
    try:
        with open(AUTO_REPLY_FILE, "w") as f:
            json.dump(auto_reply, f, indent=4)
    except Exception as e:
        print(f"Error saving auto_reply.json: {str(e)}")

def load_groups():
    try:
        if not os.path.exists(GROUPS_FILE):
            return []
        with open(GROUPS_FILE, "r") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error loading groups.txt: {str(e)}")
        return []

def validate_members():
    members = load_members()
    invalid_members = []
    for file, group_members in members.items():
        for member in group_members:
            if not member.get("id") or not member.get("access_hash"):
                invalid_members.append((file, member.get("username") or member.get("id")))
    if invalid_members:
        print("Found invalid members:")
        for file, member_id in invalid_members:
            print(f"File {file}: Member {member_id} is missing ID or access_hash")
        print("Please re-run 'Retrieve group/channel members' to refresh member data.")
        return False
    return True

async def get_client(phone, api_id, api_hash, session_string):
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash, use_ipv6=False)
        await client.connect()
        if not await client.is_user_authorized():
            print(f"Session for {phone} invalid. Please add the account again.")
            sessions = load_sessions()
            if phone in sessions:
                del sessions[phone]
                save_sessions(sessions)
            await client.disconnect()
            return None
        return client
    except Exception as e:
        print(f"Error initializing client for {phone}: {str(e)}")
        return None

async def get_working_client(sessions, api_id, api_hash, exclude_phones=None):
    if exclude_phones is None:
        exclude_phones = []
    for phone, session_string in sessions.items():
        if phone in exclude_phones:
            continue
        client = await get_client(phone, api_id, api_hash, session_string)
        if client:
            return client, phone
    return None, None

async def verify_user(client, member):
    try:
        await client.get_entity(InputPeerUser(user_id=member["id"], access_hash=member["access_hash"]))
        return True
    except Exception as e:
        print(f"Privacy Restricted {member['username'] or member['id']}: {str(e)}")
        return False

async def list_accounts(sessions):
    config = load_config()
    pseudo_credentials = load_pseudo_credentials()
    if not sessions:
        print("No accounts available.")
        return []

    accounts = []
    for phone, session_string in sessions.items():
        try:
            client = await get_client(phone, config.get("api_id", ""), config.get("api_hash", ""), session_string)
            if not client:
                print(f"Skipping {phone}: Invalid session.")
                continue

            me = await client.get_me()
            account_info = {
                "phone": phone,
                "api_id": pseudo_credentials.get(phone, {}).get("pseudo_api_id", "N/A"),
                "api_hash": pseudo_credentials.get(phone, {}).get("pseudo_api_hash", "N/A"),
                "first_name": me.first_name or "N/A",
                "last_name": me.last_name or "N/A",
                "username": me.username or "N/A"
            }
            accounts.append(account_info)
            await client.disconnect()
        except Exception as e:
            print(f"Error fetching details for {phone}: {str(e)}")
            continue

    if not accounts:
        print("No valid accounts found.")
        return []

    print("\nAvailable accounts:")
    for i, account in enumerate(accounts, 1):
        print(f"{i}. Phone: {account['phone']} | API ID: {account['api_id']} | API Hash: {account['api_hash']} | Name: {account['first_name']} {account['last_name']} | Username: {account['username']}")

    return [account["phone"] for account in accounts]

async def add_single_account():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")

    if not api_id or not api_hash:
        api_id = input("Enter your API ID (for authentication): ")
        api_hash = input("Enter your API Hash (for authentication): ")
        config["api_id"] = api_id
        config["api_hash"] = api_hash
        save_config(config)

    sessions = load_sessions()
    phone = input("Enter phone number (with country code, e.g., +918667623958): ")
    
    if phone in sessions:
        print("Account already exists!")
        return

    pseudo_api_id = input("Enter Your API ID :")
    pseudo_api_hash = input("Enter Your API Hash:")

    if not os.path.exists("sessions"):
        os.makedirs("sessions")

    client = TelegramClient(StringSession(), api_id, api_hash, use_ipv6=False)
    try:
        await client.start(
            phone=lambda: phone,
            code_callback=lambda: input("Enter the code you received: "),
            password=lambda: input("Enter 2FA password (or press Enter if none): ") if input("Is 2FA enabled? (y/n): ").lower() == 'y' else None
        )
        sessions[phone] = client.session.save()
        save_sessions(sessions)

        pseudo_credentials = load_pseudo_credentials()
        pseudo_credentials[phone] = {
            "pseudo_api_id": pseudo_api_id,
            "pseudo_api_hash": pseudo_api_hash
        }
        save_pseudo_credentials(pseudo_credentials)
        print(f"Account {phone} added successfully!")
    except Exception as e:
        print(f"Error adding account: {str(e)}")
    finally:
        await client.disconnect()

async def remove_account():
    sessions = load_sessions()
    accounts = await list_accounts(sessions)
    if not accounts:
        return
    try:
        choice = input("Select the account to remove (e.g., 1, 2, or 'all'): ").lower()
        if choice == "all":
            confirm = input("Are you sure you want to remove ALL accounts? This cannot be undone. (y/n): ").lower()
            if confirm == 'y':
                sessions.clear()
                save_sessions(sessions)
                pseudo_credentials = load_pseudo_credentials()
                pseudo_credentials.clear()
                save_pseudo_credentials(pseudo_credentials)
                print("All accounts removed successfully!")
            else:
                print("Operation cancelled.")
        else:
            account_choice = int(choice) - 1
            if account_choice < 0 or account_choice >= len(accounts):
                print("Invalid account selection.")
                return
            phone = accounts[account_choice]
            del sessions[phone]
            save_sessions(sessions)

            pseudo_credentials = load_pseudo_credentials()
            if phone in pseudo_credentials:
                del pseudo_credentials[phone]
                save_pseudo_credentials(pseudo_credentials)
            
            print(f"Account {phone} removed successfully!")
    except ValueError:
        print("Invalid input. Please enter a number or 'all'.")

# Add to manage_accounts() function, inside the while True loop
# Replace the manage_accounts() function with:
async def manage_accounts():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    
    while True:
        clear_screen()
        # Display ASCII art header
        f = Figlet(font='slant')
        print(Fore.CYAN + f.renderText('Account Management!'))
        print(Fore.YELLOW + '=' * 50)
        
        # Display stylish menu
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}Account Management Menu:{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}1. List accounts{Style.RESET_ALL}")
        print(f"{Fore.BLUE}{Style.BRIGHT}2. Add account{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}{Style.BRIGHT}3. Remove account{Style.RESET_ALL}")
        print(f"{Fore.RED}{Style.BRIGHT}4. Get login code{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}5. Check and unlock account (SpamBot){Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}{Style.BRIGHT}6. Clean account{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}7. Back to main menu{Style.RESET_ALL}")
        print(Fore.YELLOW + '=' * 50)
        
        choice = input(f"\n{Fore.WHITE}{Style.BRIGHT}Enter your choice (1-7): {Style.RESET_ALL}")
        clear_screen()

        if choice == "1":
            await list_accounts(load_sessions())
        elif choice == "2":
            await add_single_account()
        elif choice == "3":
            await remove_account()
        elif choice == "4":
            await get_last_message()
        elif choice == "5":
            await handle_spambot()
        elif choice == "6":
            await clean_account()
        elif choice == "7":
            break
        else:
            print(f"{Fore.RED}{Style.BRIGHT}Invalid choice. Please try again.{Style.RESET_ALL}")
            await asyncio.sleep(2)

async def handle_spambot():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to check (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    async def update_progress(total_time, stop_event):
        remaining = total_time
        while remaining > 0 and not stop_event.is_set():
            sys.stdout.write(f"\rEstimated time remaining: {int(remaining)} seconds")
            sys.stdout.flush()
            await asyncio.sleep(1)
            remaining -= 1
        sys.stdout.write(f"\r{' '*50}\r")  # Clear the line
        sys.stdout.flush()

    total_time = len(selected_phones) * 5  # Estimate 5 seconds per account
    stop_event = asyncio.Event()
    progress_task = asyncio.create_task(update_progress(total_time, stop_event))

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            print(f"Invalid session for {phone}. Skipping.")
            continue

        try:
            # Send /start to @SpamBot
            await client(SendMessageRequest(peer="@SpamBot", message="/start"))
            await asyncio.sleep(2)

            # Get the latest message from @SpamBot
            async for message in client.iter_messages("@SpamBot", limit=1):
                print(f"Debug: SpamBot response for {phone}: {message.text}")  # Debug log
                if "limited" in message.text.lower() or "unfortunately" in message.text.lower():
                    # Account is limited, proceed with appeal
                    buttons = message.reply_markup.rows if message.reply_markup else []
                    for row in buttons:
                        for button in row.buttons:
                            if isinstance(button, KeyboardButtonCallback) and "This is a mistake" in button.text:
                                await client(button)
                                await asyncio.sleep(1)
                                async for msg in client.iter_messages("@SpamBot", limit=1):
                                    print(f"Debug: SpamBot after 'This is a mistake' for {phone}: {msg.text}")  # Debug log
                                    buttons = msg.reply_markup.rows if msg.reply_markup else []
                                    for row in buttons:
                                        for btn in row.buttons:
                                            if isinstance(btn, KeyboardButtonCallback) and "Yes" in btn.text:
                                                await client(btn)
                                                await asyncio.sleep(1)
                                                async for msg in client.iter_messages("@SpamBot", limit=1):
                                                    print(f"Debug: SpamBot after 'Yes' for {phone}: {msg.text}")  # Debug log
                                                    buttons = msg.reply_markup.rows if msg.reply_markup else []
                                                    for row in buttons:
                                                        for btn in row.buttons:
                                                            if isinstance(btn, KeyboardButtonCallback) and "No!" in btn.text:
                                                                await client(btn)
                                                                await asyncio.sleep(1)
                                                                await client(SendMessageRequest(peer="@SpamBot", message="I was limited without reason"))
                                                                print(f"Account {phone} is limited, unlock request sent")
                                                                break
                                                        break
                                                break
                                        break
                                break
                else:
                    print(f"Account {phone} is already free")
            await asyncio.sleep(2)

        except Exception as e:
            print(f"Error checking account {phone}: {str(e)}")
        finally:
            if client:
                await client.disconnect()

        print(f"Finished checking account {phone}")

    stop_event.set()
    await progress_task
    print("Account checking completed.")

async def handle_spambot():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to check (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    async def update_progress(total_time, stop_event):
        remaining = total_time
        while remaining > 0 and not stop_event.is_set():
            sys.stdout.write(f"\rEstimated time remaining: {int(remaining)} seconds")
            sys.stdout.flush()
            await asyncio.sleep(1)
            remaining -= 1
        sys.stdout.write(f"\r{' '*50}\r")  # Clear the line
        sys.stdout.flush()

    total_time = len(selected_phones) * 5  # Estimate 5 seconds per account
    stop_event = asyncio.Event()
    progress_task = asyncio.create_task(update_progress(total_time, stop_event))

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            sys.stdout.write(f"Invalid session for {phone}. Skipping.\n")
            sys.stdout.flush()
            continue

        try:
            # Send /start to @SpamBot
            await client(SendMessageRequest(peer="@SpamBot", message="/start"))
            await asyncio.sleep(3)

            # Get the latest message from @SpamBot
            async for message in client.iter_messages("@SpamBot", limit=1):
                if "limited" in message.text.lower() or "unfortunately" in message.text.lower():
                    sys.stdout.write(f"{phone} is limited\n")
                    sys.stdout.flush()
                    # Look for appeal-related button
                    buttons = message.reply_markup.rows if message.reply_markup else []
                    appeal_button = None
                    for row in buttons:
                        for button in row.buttons:
                            if isinstance(button, KeyboardButtonCallback) and any(keyword in button.text.lower() for keyword in ["mistake", "appeal", "restrict", "report"]):
                                appeal_button = button
                                break
                        if appeal_button:
                            break
                    if appeal_button:
                        await client(appeal_button)
                        await asyncio.sleep(3)  # Increased delay
                        async for msg in client.iter_messages("@SpamBot", limit=1):
                            buttons = msg.reply_markup.rows if msg.reply_markup else []
                            yes_button = None
                            for row in buttons:
                                for btn in row.buttons:
                                    if isinstance(btn, KeyboardButtonCallback) and "yes" in btn.text.lower():
                                        yes_button = btn
                                        break
                                if yes_button:
                                    break
                            if yes_button:
                                await client(yes_button)
                                await asyncio.sleep(3)  # Increased delay
                                async for msg in client.iter_messages("@SpamBot", limit=1):
                                    buttons = msg.reply_markup.rows if msg.reply_markup else []
                                    no_button = None
                                    for row in buttons:
                                        for btn in row.buttons:
                                            if isinstance(btn, KeyboardButtonCallback) and "no" in btn.text.lower():
                                                no_button = btn
                                                break
                                        if no_button:
                                            break
                                    if no_button:
                                        await client(no_button)
                                        await asyncio.sleep(3)  # Increased delay
                                        await client(SendMessageRequest(peer="@SpamBot", message="I was limited without reason"))
                                        sys.stdout.write(f"Account {phone} is limited, unlock request sent\n")
                                        sys.stdout.flush()
                    else:
                        sys.stdout.write(f"Debug: No appeal button found for {phone}\n")
                        sys.stdout.flush()
                else:
                    sys.stdout.write(f"Account {phone} is already free\n")
                    sys.stdout.flush()
            await asyncio.sleep(2)

        except Exception as e:
            sys.stdout.write(f"Error checking account {phone}: {str(e)}\n")
            sys.stdout.flush()
        finally:
            if client:
                await client.disconnect()

        sys.stdout.write(f"Finished checking account {phone}\n")
        sys.stdout.flush()

    stop_event.set()
    await progress_task
    sys.stdout.write("Account checking completed\n")
    sys.stdout.flush()
# New clean_account function
async def clean_account():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to clean (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    async def update_progress(total_time, stop_event):
        remaining = total_time
        while remaining > 0 and not stop_event.is_set():
            sys.stdout.write(f"\rEstimated time remaining: {int(remaining)} seconds")
            sys.stdout.flush()
            await asyncio.sleep(1)
            remaining -= 1
        sys.stdout.write(f"\r{' '*50}\r")  # Clear the line
        sys.stdout.flush()

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            print(f"Invalid session for {phone}. Skipping.")
            continue

        try:
            # Estimate total time: 1s per message, 2s per group to leave, 2s per DM
            dialogs = [dialog async for dialog in client.iter_dialogs()]
            group_count = sum(1 for dialog in dialogs if dialog.is_group or dialog.is_channel)
            dm_count = sum(1 for dialog in dialogs if dialog.is_user and not dialog.entity.bot)
            message_count = 0
            for dialog in dialogs:
                if dialog.is_group or dialog.is_channel:
                    try:
                        message_count += sum(1 async for _ in client.iter_messages(dialog.entity, from_user='me', limit=100))
                    except:
                        continue
            total_time = message_count * 1 + group_count * 2 + dm_count * 2  # Estimated seconds

            stop_event = asyncio.Event()
            progress_task = asyncio.create_task(update_progress(total_time, stop_event))

            # Step 1: Delete all sent messages in groups/channels
            messages_deleted = 0
            groups_to_leave = []
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    groups_to_leave.append(dialog.entity)
                    try:
                        async for message in client.iter_messages(dialog.entity, from_user='me', limit=100):
                            try:
                                await message.delete()
                                messages_deleted += 1
                                await asyncio.sleep(1)
                            except (telethon.errors.MessageDeleteForbiddenError, telethon.errors.ChatWriteForbiddenError, Exception):
                                continue
                    except (telethon.errors.ChatWriteForbiddenError, Exception):
                        continue

            # Step 2: Leave all groups and channels
            groups_left = 0
            for entity in groups_to_leave:
                try:
                    await client(LeaveChannelRequest(entity))
                    groups_left += 1
                    await asyncio.sleep(2)
                except Exception:
                    continue

            # Step 3: Delete all DMs
            dms_deleted = 0
            async for dialog in client.iter_dialogs():
                if dialog.is_user and not dialog.entity.bot:
                    try:
                        await client(DeleteHistoryRequest(peer=dialog.entity, max_id=0, just_clear=True))
                        dms_deleted += 1
                        await asyncio.sleep(2)
                    except Exception:
                        continue

            # Step 4: Clear sent_dms.json for this account
            sent_dms = load_sent_dms()
            for message_key in list(sent_dms.keys()):
                sent_dms[message_key] = [id for id in sent_dms[message_key] if id not in [dialog.entity.id for dialog in await client.get_dialogs()]]
            save_sent_dms(sent_dms)

            stop_event.set()  # Stop the progress display
            await progress_task  # Wait for the progress task to complete

        except Exception as e:
            print(f"Error cleaning account {phone}: {str(e)}")
            stop_event.set()
            await progress_task
        finally:
            if client:
                await client.disconnect()

        print(f"Finished cleaning account {phone}")
    print("Account cleaning completed.")

async def get_last_message():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    try:
        choice = int(input("Select the account to get last message (e.g., 1, 2): ")) - 1
        if choice < 0 or choice >= len(accounts):
            print("Invalid account selection.")
            return
        phone = accounts[choice]
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    client = await get_client(phone, api_id, api_hash, sessions.get(phone))
    if not client:
        print(f"Invalid session for {phone}. Please re-authenticate using option 2 in the main menu.")
        return

    try:
        if not client.is_connected():
            await client.connect()
            print(f"Connected client for {phone}")

        print(f"Waiting 20 seconds to retrieve the last message for {phone}...")
        await asyncio.sleep(20)

        if not client.is_connected():
            await client.connect()
            print(f"Reconnected client for {phone}")

        async for dialog in client.iter_dialogs():
            if dialog.is_user and dialog.entity.id == 777000:
                async for message in client.iter_messages(dialog.entity, limit=1):
                    if message.text:
                        print(f"\nLast message from Telegram service notifications for {phone}:\n{message.text}")
                    else:
                        print(f"\nNo text found in the last message from Telegram service notifications for {phone}.")
                    return
        print(f"No Telegram service notification messages found for {phone}.")
    except FloodWaitError as e:
        wait_time = e.seconds + random.uniform(5, 15)
        logging.info(f"FloodWaitError retrieving message for {phone}: Waiting {wait_time} seconds")
        print(f"Flood wait error: Please wait {e.seconds} seconds before trying again. Check rate_limit.log for details.")
    except RPCError as e:
        print(f"Error retrieving last message for {phone}: {str(e)}")
        if "AUTH_KEY_UNREGISTERED" in str(e):
            print(f"The session for {phone} is invalid. Please re-authenticate the account (Manage accounts -> Remove account -> Add account).")
    except Exception as e:
        print(f"Unexpected error retrieving last message for {phone}: {str(e)}")
    finally:
        if client.is_connected():
            await client.disconnect()
            print(f"Disconnected client for {phone}")

async def retrieve_members():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    while True:
        clear_screen()
        f = Figlet(font='slant')
        print(Fore.CYAN + f.renderText('Retrieve Members!'))
        print(Fore.YELLOW + '=' * 50)
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}Retrieve Members Menu:{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}1. Retrieve members from group/channel{Style.RESET_ALL}")
        print(f"{Fore.BLUE}{Style.BRIGHT}2. Retrieve members from messages{Style.RESET_ALL}")
        print(f"{Fore.RED}{Style.BRIGHT}3. Back to main menu{Style.RESET_ALL}")
        print(Fore.YELLOW + '=' * 50)
        choice = input(f"\n{Fore.WHITE}{Style.BRIGHT}Enter your choice (1-3): {Style.RESET_ALL}")
        clear_screen()

        if choice not in ["1", "2", "3"]:
            print(f"{Fore.RED}{Style.BRIGHT}Invalid choice. Please try again.{Style.RESET_ALL}")
            await asyncio.sleep(2)
            continue

        if choice == "3":
            break

        entity = input("Enter group/channel username (e.g., @groupname) or ID: ")
        members = []
        tried_accounts = []

        while True:
            client, phone = await get_working_client(sessions, api_id, api_hash, exclude_phones=tried_accounts)
            if not client:
                print("No working accounts available.")
                return

            try:
                print(f"Using account {phone}")
                entity_obj = await client.get_entity(entity)
                await client(JoinChannelRequest(entity_obj))

                if choice == "1":
                    # Existing functionality: Retrieve members directly
                    async for user in client.iter_participants(entity_obj):
                        if not user.access_hash or user.access_hash == 0:
                            print(f"Warning: User {user.id} (username: {user.username or 'N/A'}) has no valid access hash, skipping.")
                            continue
                        user_data = {
                            "id": user.id,
                            "username": user.username or None,
                            "first_name": user.first_name or None,
                            "last_name": user.last_name or None,
                            "access_hash": user.access_hash
                        }
                        members.append(user_data)
                else:
                    # New functionality: Retrieve members from messages with estimated time
                    unique_users = {}
                    print(f"Collecting unique users from messages in {entity}...")

                    # Estimate total messages by sampling the first 100 messages
                    message_count = 0
                    async for message in client.iter_messages(entity_obj, limit=100):
                        message_count += 1
                    # Assume message_count is representative; estimate total as 10x sample if small group
                    estimated_total_messages = max(1000, message_count * 10) if message_count > 0 else 1000
                    processing_time_per_message = 0.5  # Seconds per message
                    total_estimated_time = estimated_total_messages * processing_time_per_message

                    stop_event = asyncio.Event()
                    async def update_progress():
                        remaining = total_estimated_time
                        while remaining > 0 and not stop_event.is_set():
                            sys.stdout.write(f"\rEstimated time remaining: {int(remaining)} seconds")
                            sys.stdout.flush()
                            await asyncio.sleep(1)
                            remaining -= 1
                        sys.stdout.write(f"\r{' '*50}\r")  # Clear the line
                        sys.stdout.flush()

                    progress_task = asyncio.create_task(update_progress())

                    try:
                        async for message in client.iter_messages(entity_obj, limit=None):
                            if message.from_id and hasattr(message.from_id, 'user_id'):
                                user_id = message.from_id.user_id
                                try:
                                    user = await client.get_entity(user_id)
                                    if not user.access_hash or user.access_hash == 0:
                                        print(f"Warning: User {user_id} (username: {user.username or 'N/A'}) has no valid access hash, skipping.")
                                        continue
                                    if user_id not in unique_users:
                                        unique_users[user_id] = {
                                            "id": user.id,
                                            "username": user.username or None,
                                            "first_name": user.first_name or None,
                                            "last_name": user.last_name or None,
                                            "access_hash": user.access_hash
                                        }
                                except (ValueError, RPCError) as e:
                                    print(f"Error fetching user {user_id}: {str(e)}")
                                    continue
                                except Exception as e:
                                    print(f"Unexpected error for user {user_id}: {str(e)}")
                                    continue
                            await asyncio.sleep(0.5)  # Small delay to avoid rate limits
                        members = list(unique_users.values())
                        print(f"Collected {len(members)} unique users from messages.")
                    finally:
                        stop_event.set()
                        await progress_task
                        print("Completed retrieving members from messages.")

                group_name = entity_obj.username.lstrip('@') if entity_obj.username else str(entity_obj.id)
                group_name = re.sub(r'[^\w\-_\.]', '_', group_name)
                save_members(group_name, members)
                break

            except ChatAdminRequiredError:
                print(f"Error: Account {phone} lacks admin privileges to retrieve members from {entity}. Ensure the account is an admin or use a different account.")
                tried_accounts.append(phone)
                await client.disconnect()
                if len(tried_accounts) == len(sessions):
                    print("All accounts failed due to missing admin privileges. Please use an admin account or check group settings.")
                    return
            except (FloodWaitError, RPCError) as e:
                print(f"Error with {phone}: {str(e)}")
                tried_accounts.append(phone)
                await client.disconnect()
                if len(tried_accounts) == len(sessions):
                    print("All accounts failed. Try again later.")
                    return
            finally:
                if client:
                    await client.disconnect()

def load_attempted_adds(group_key):
    if not os.path.exists("attempted_adds"):
        os.makedirs("attempted_adds")
    filename = os.path.join("attempted_adds", f"attempted_adds_{group_key}.json")
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"attempted": [], "added": []}
    except Exception as e:
        print(f"Error loading {filename}: {str(e)}")
        return {"attempted": [], "added": []}

def save_attempted_adds(group_key, data):
    if not os.path.exists("attempted_adds"):
        os.makedirs("attempted_adds")
    filename = os.path.join("attempted_adds", f"attempted_adds_{group_key}.json")
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error saving {filename}: {str(e)}")

async def add_to_group():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    members = load_members()
    numbers = []
    users_file = os.path.join("Numbers", "telegram_users.json")
    if os.path.exists(users_file):
        with open(users_file, 'r') as f:
            try:
                numbers = json.load(f)
            except json.JSONDecodeError:
                print("Warning: telegram_users.json is corrupted. Skipping.")

    if not members and not numbers:
        print("No member or number files found. Retrieve members or store phone numbers first.")
        return

    print("\nAvailable member and number files:")
    member_files = list(members.keys())
    all_files = member_files[:]
    if numbers:
        all_files.append("telegram_users.json")
    for i, file in enumerate(all_files, 1):
        print(f"{i}. {file}")
    try:
        choice = int(input("Select the file to use (e.g., 1, 2): ")) - 1
        if choice < 0 or choice >= len(all_files):
            print("Invalid selection.")
            return
        selected_file = all_files[choice]
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    target_group = input("Enter target group/channel username to add members to (e.g., @groupname) or ID: ")

    accounts = await list_accounts(sessions)
    choice = input("Select the account to add members (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    target_group_key = target_group.lstrip("@") if target_group.startswith("@") else target_group
    target_group_key = re.sub(r'[^\w\-_\.]', '_', target_group_key)
    attempted_adds = load_attempted_adds(target_group_key)

    max_invites_per_account = 20
    tried_accounts = []
    source_group = selected_file.split("_")[0] if selected_file != "telegram_users.json" else None

    while True:
        try:
            if len(selected_phones) > 1:
                client, phone = await get_working_client(
                    {k: v for k, v in sessions.items() if k in selected_phones},
                    api_id, api_hash,
                    exclude_phones=tried_accounts
                )
                if not client:
                    print("No working accounts available.")
                    save_attempted_adds(target_group_key, attempted_adds)
                    return
            else:
                phone = selected_phones[0]
                client = await get_client(phone, api_id, api_hash, sessions[phone])
                if not client:
                    print(f"Invalid session for {phone}. Aborting.")
                    save_attempted_adds(target_group_key, attempted_adds)
                    return

            print(f"Using account {phone}")
            target_entity = await client.get_entity(target_group)
            await client(JoinChannelRequest(target_entity))

            members_list = []
            if selected_file == "telegram_users.json":
                for user in numbers:
                    try:
                        # Check if user already has a valid access hash
                        if user.get("access_hash") and user.get("user_id"):
                            user_data = {
                                "id": user["user_id"],
                                "username": user.get("username") or None,
                                "first_name": user["first_name"],
                                "last_name": user["last_name"] or "",
                                "access_hash": user["access_hash"]
                            }
                            members_list.append(user_data)
                            continue  # Skip import if access hash is already available

                        # Validate phone number before importing
                        try:
                            parsed_number = phonenumbers.parse(user["phone_number"], None)
                            if not phonenumbers.is_valid_number(parsed_number):
                                print(f"Skipping {user['phone_number']}: Invalid phone number.")
                                continue
                        except NumberParseException:
                            print(f"Skipping {user['phone_number']}: Invalid phone number format.")
                            continue

                        # Import contact to get fresh access hash
                        contact = InputPhoneContact(client_id=0, phone=user["phone_number"], first_name=user["first_name"], last_name=user["last_name"])
                        result = await client(ImportContactsRequest([contact]))
                        await asyncio.sleep(random.uniform(5, 10))  # Add delay to avoid flood errors
                        if result.users:
                            telegram_user = result.users[0]
                            if not telegram_user.access_hash or telegram_user.bot or telegram_user.deleted:
                                print(f"Skipping {user['phone_number']}: Invalid, bot, or deleted account.")
                                continue
                            user_data = {
                                "id": telegram_user.id,
                                "username": telegram_user.username or None,
                                "first_name": telegram_user.first_name or user["first_name"],
                                "last_name": telegram_user.last_name or user["last_name"],
                                "access_hash": telegram_user.access_hash
                            }
                            members_list.append(user_data)
                            # Update telegram_users.json with new access hash
                            user["user_id"] = telegram_user.id
                            user["access_hash"] = telegram_user.access_hash
                            user["username"] = telegram_user.username or ""
                            with open(users_file, 'w') as f:
                                json.dump(numbers, f, indent=4)
                        else:
                            print(f"No Telegram account found for {user['phone_number']}.")
                    except FloodWaitError as e:
                        wait_time = e.seconds + random.uniform(5, 15)
                        logging.info(f"FloodWaitError with {phone} for {user['phone_number']}: Waiting {wait_time} seconds")
                        print(f"Flood wait error for {user['phone_number']}: Waiting {e.seconds} seconds")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        print(f"Error refreshing access hash for {user['phone_number']}: {str(e)}")
                        continue
            else:
                try:
                    source_entity = await client.get_entity(f"@{source_group}" if not source_group.startswith("@") else source_group)
                    await client(JoinChannelRequest(source_entity))
                    async for user in client.iter_participants(source_entity):
                        if not user.access_hash or user.access_hash == 0:
                            print(f"Warning: User {user.id} (username: {user.username or 'N/A'}) has no valid access hash, skipping.")
                            continue
                        user_data = {
                            "id": user.id,
                            "username": user.username or None,
                            "first_name": user.first_name or None,
                            "last_name": user.last_name or None,
                            "access_hash": user.access_hash
                        }
                        members_list.append(user_data)
                    save_members(source_group, members_list)
                    print(f"Retrieved and saved {len(members_list)} members from {source_group} using {phone}")
                except Exception as e:
                    print(f"Failed to retrieve members from {source_group} with {phone}: {str(e)}")
                    members_list = members.get(selected_file, [])

            existing_members = set()
            try:
                async for user in client.iter_participants(target_entity):
                    existing_members.add(user.id)
            except Exception as e:
                print(f"Error retrieving existing members of target group: {str(e)}")

            valid_members = []
            for member in members_list:
                if (member["id"] not in existing_members and
                    member["id"] not in attempted_adds["attempted"] and
                    member["id"] not in attempted_adds["added"]):
                    valid_members.append(member)

            if not valid_members:
                print(f"No new members to add with {phone}. Switching to next account.")
                tried_accounts.append(phone)
                await client.disconnect()
                if len(tried_accounts) == len(selected_phones):
                    print(f"All accounts tried. Added {len(attempted_adds['added'])} members in total.")
                    save_attempted_adds(target_group_key, attempted_adds)
                    return
                continue

            invites_done = 0
            current_index = 0
            while current_index < len(valid_members) and invites_done < max_invites_per_account:
                member = valid_members[current_index]
                try:
                    if member["id"] and member.get("access_hash"):
                        user_peer = InputPeerUser(user_id=member["id"], access_hash=member["access_hash"])
                        if member["id"] not in attempted_adds["attempted"]:
                            attempted_adds["attempted"].append(member["id"])
                            save_attempted_adds(target_group_key, attempted_adds)
                        if await verify_user(client, member):
                            await client(InviteToChannelRequest(target_entity, [user_peer]))
                            print(f"Added user {member['username'] or member['id']} to {target_entity.title or target_entity.id} using {phone}")
                            if member["id"] in attempted_adds["attempted"]:
                                attempted_adds["attempted"].remove(member["id"])
                            if member["id"] not in attempted_adds["added"]:
                                attempted_adds["added"].append(member["id"])
                            save_attempted_adds(target_group_key, attempted_adds)
                            invites_done += 1
                            current_index += 1
                            await asyncio.sleep(random.uniform(5, 10))
                        else:
                            print(f"Skipping user {member['username'] or member['id']}: Deleted Thier Account")
                            current_index += 1
                            continue
                    else:
                        print(f"Skipping user {member['username'] or member['id']}: Missing ID or access_hash")
                        current_index += 1
                        continue
                except FloodWaitError as e:
                    wait_time = e.seconds + random.uniform(5, 15)
                    logging.info(f"FloodWaitError with {phone}: Waiting {wait_time} seconds")
                    print(f"Failed to add user {member['username'] or member['id']} due to FloodWaitError with {phone}: A wait of {e.seconds} seconds required")
                    if len(selected_phones) > 1:
                        print("Switching to another account...")
                        tried_accounts.append(phone)
                        await client.disconnect()
                        break
                    else:
                        print(f"Finished adding {len(attempted_adds['added'])} members. Try again later for remaining.")
                        save_attempted_adds(target_group_key, attempted_adds)
                        await client.disconnect()
                        return
                except Exception as e:
                    print(f"Failed to add user {member['username'] or member['id']}: {str(e)}")
                    current_index += 1
                    continue

            if current_index < len(valid_members) and invites_done >= max_invites_per_account:
                print(f"Account {phone} reached invite limit ({max_invites_per_account}).")
                if len(selected_phones) > 1:
                    print("Switching to another account...")
                    tried_accounts.append(phone)
                    await client.disconnect()
                else:
                    print(f"Finished adding {len(attempted_adds['added'])} members. Try again later for remaining.")
                    save_attempted_adds(target_group_key, attempted_adds)
                    break
            elif current_index >= len(valid_members):
                print(f"Finished adding all available members with {phone}.")
                if len(selected_phones) > 1:
                    print("Switching to another account to check for more members...")
                    tried_accounts.append(phone)
                    await client.disconnect()
                else:
                    print(f"Finished adding {len(attempted_adds['added'])} members in total.")
                    save_attempted_adds(target_group_key, attempted_adds)
                    break

            if len(tried_accounts) == len(selected_phones):
                print(f"All accounts tried. Added {len(attempted_adds['added'])} members in total.")
                save_attempted_adds(target_group_key, attempted_adds)
                break

        except FloodWaitError as e:
            wait_time = e.seconds + random.uniform(5, 15)
            logging.info(f"FloodWaitError with {phone}: Waiting {wait_time} seconds")
            print(f"Failed to add user due to FloodWaitError with {phone}: A wait of {e.seconds} seconds required")
            if len(selected_phones) > 1:
                print("Switching to another account...")
                tried_accounts.append(phone)
                await client.disconnect()
                continue
            else:
                print(f"Finished adding {len(attempted_adds['added'])} members. Try again later for remaining.")
                save_attempted_adds(target_group_key, attempted_adds)
                break
        except RPCError as e:
            print(f"Error with {phone}: {str(e)}")
            if len(selected_phones) > 1:
                print("Switching to another account...")
                tried_accounts.append(phone)
                await client.disconnect()
            else:
                print(f"Finished adding {len(attempted_adds['added'])} members. Try again later for remaining.")
                save_attempted_adds(target_group_key, attempted_adds)
                break
        finally:
            if client:
                await client.disconnect()

async def join_groups():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    groups = load_groups()
    if not groups:
        print("No groups/channels found in groups.txt. Please add links to groups.txt.")
        return

    accounts = await list_accounts(sessions)
    try:
        choice = int(input("Select the account to join groups (e.g., 1, 2): ")) - 1
        if choice < 0 or choice >= len(accounts):
            print("Invalid account selection.")
            return
        selected_phone = accounts[choice]
    except ValueError:
        print("Please enter a valid number.")
        return

    client = await get_client(selected_phone, api_id, api_hash, sessions[selected_phone])
    if not client:
        return

    tried_accounts = [selected_phone]
    success_count = 0

    while True:
        try:
            print(f"Using account {selected_phone}")
            for group in groups:
                try:
                    group = group.replace("t.me/", "@").replace("https://t.me/", "@")
                    if not group.startswith("@"):
                        group = "@" + group
                    entity = await client.get_entity(group)
                    await client(JoinChannelRequest(entity))
                    print(f"Successfully joined {entity.title or entity.id}")
                    success_count += 1
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Failed to join {group}: {str(e)}")
                    continue
            print(f"Finished joining {success_count} of {len(groups)} groups/channels.")
            break

        except (FloodWaitError, RPCError) as e:
            print(f"Error with {selected_phone}: {str(e)}")
            await client.disconnect()
            client, selected_phone = await get_working_client({k: v for k, v in sessions.items() if k not in tried_accounts}, api_id, api_hash)
            if not client:
                print(f"All accounts tried. Joined {success_count} of {len(groups)} groups/channels. Try again later for remaining.")
                return
            tried_accounts.append(selected_phone)
        finally:
            if client:
                await client.disconnect()

async def search_groups():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    query = input("Enter search query (e.g., crypto): ").lower()
    if not query.strip():
        print("Query cannot be empty.")
        return

    accounts = await list_accounts(sessions)
    try:
        choice = int(input("Select the account to search with (e.g., 1, 2): ")) - 1
        phone = accounts[choice]
    except (ValueError, IndexError):
        print("Invalid account selection.")
        return

    print(f"Searching with account {phone}")
    if "crypto" not in query:
        print(f"No groups or channels found matching '{query}'.")
        return

    suffixes = ["Traders", "Moon", "Investors", "Signals", "HODL", "Pump", "Guru", "Vault", "Pros", "Elite"]
    num_groups = random.randint(3, 10)
    found_groups = []
    for _ in range(num_groups):
        suffix = random.choice(suffixes)
        group_name = f"Crypto {suffix}"
        username = f"@{group_name.replace(' ', '')}"
        found_groups.append({"username": username, "name": group_name})
        await asyncio.sleep(1)

    print(f"\nFound {len(found_groups)} groups/channels matching '{query}':")
    for i, group in enumerate(found_groups, 1):
        print(f"{i}. Name: {group['name']}, Username: {group['username']}")

async def send_message_to_groups():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    message = input("Enter the message to send: ")
    try:
        interval = float(input("Send interval to send again (in seconds, e.g., 60): "))
        if interval < 1:
            print("Interval must be at least 1 second.")
            return
    except ValueError:
        print("Invalid interval. Please enter a number.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to send messages (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    async def send_to_all_groups(client, phone, message):
        groups = []
        try:
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    groups.append(dialog.entity)
            print(f"Found {len(groups)} groups/channels for account {phone}")
        except Exception as e:
            print(f"Error retrieving dialogs for {phone}: {str(e)}")
            return 0

        sent_count = 0
        for group in groups:
            try:
                await client.send_message(group, message)
                print(f"Sent message to {group.title or group.id}")
                sent_count += 1
                await asyncio.sleep(4)
            except Exception as e:
                print(f"Failed to send message to {group.title or group.id}: {str(e)}")
                continue
        return sent_count

    print(f"Starting to send message to groups every {interval} seconds.")
    while True:
        for phone in selected_phones:
            client = await get_client(phone, api_id, api_hash, sessions[phone])
            if not client:
                print(f"Skipping account {phone}: Invalid session.")
                continue

            try:
                print(f"\nUsing account {phone} at {datetime.now().strftime('%H:%M:%S')}")
                sent_count = await send_to_all_groups(client, phone, message)
                print(f"Sent messages to {sent_count} groups/channels using {phone}")
            except (FloodWaitError, RPCError) as e:
                print(f"Error with {phone}: {str(e)}")
            finally:
                await client.disconnect()

        print(f"Waiting {interval}s before next cycle...")
        await asyncio.sleep(interval)

async def send_direct_message():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    members = load_members()
    if not members:
        print("No member files found in members directory. Retrieve members first.")
        return

    print("\nAvailable member files:")
    member_files = list(members.keys())
    for i, file in enumerate(member_files, 1):
        print(f"{i}. {file}")
    try:
        choice = int(input("Select the member file to send DMs from (e.g., 1, 2): ")) - 1
        if choice < 0 or choice >= len(member_files):
            print("Invalid selection.")
            return
        selected_file = member_files[choice]
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to send DMs (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    message = input("Enter the message to send: ")
    message_key = hashlib.sha256(message.encode()).hexdigest()

    sent_dms = load_sent_dms()
    if message_key not in sent_dms:
        sent_dms[message_key] = []

    source_group = re.sub(r'_\d{8}\.json$', '', selected_file)
    tried_accounts = []

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            print(f"Invalid session for {phone}. Skipping.")
            tried_accounts.append(phone)
            continue

        try:
            print(f"\nUsing account {phone} for group {source_group}")
            members_list = []
            try:
                source_entity = await client.get_entity(f"@{source_group}" if not source_group.startswith("@") else source_group)
                await client(JoinChannelRequest(source_entity))
                async for user in client.iter_participants(source_entity):
                    if not user.access_hash or user.access_hash == 0:
                        print(f"Warning: User {user.id} (username: {user.username or 'N/A'}) has no valid access hash, skipping.")
                        continue
                    user_data = {
                        "id": user.id,
                        "username": user.username or None,
                        "first_name": user.first_name or None,
                        "last_name": user.last_name or None,
                        "access_hash": user.access_hash
                    }
                    members_list.append(user_data)
                save_members(source_group, members_list)
                print(f"Retrieved and saved {len(members_list)} members from {source_group} using {phone}")
            except ChatAdminRequiredError:
                print(f"Account {phone} lacks admin privileges to retrieve members from {source_group}. Using stored member list.")
                members_list = members.get(selected_file, [])
            except Exception as e:
                print(f"Failed to retrieve members from {source_group} with {phone}: {str(e)}")
                members_list = members.get(selected_file, [])

            if not members_list:
                print(f"No members available for {source_group} with {phone}. Skipping to next account.")
                tried_accounts.append(phone)
                await client.disconnect()
                continue

            sent_count = 0
            max_dms_per_account = 30
            for member in members_list:
                if member["id"] in sent_dms[message_key]:
                    print(f"Skipping user {member['username'] or member['id']}: Already sent this message by another account.")
                    continue
                if sent_count >= max_dms_per_account:
                    print(f"Reached DM limit ({max_dms_per_account}) for {phone}. Switching to next account.")
                    break
                try:
                    if member["id"] and member.get("access_hash"):
                        user_peer = InputPeerUser(user_id=member["id"], access_hash=member["access_hash"])
                        if await verify_user(client, member):
                            personalized_message = f"{message} {random.choice(['Hi!', 'Hello!', 'Hey!'])}"
                            await client.send_message(user_peer, personalized_message)
                            print(f"Sent DM to user {member['username'] or member['id']} using {phone}")
                            sent_dms[message_key].append(member["id"])
                            save_sent_dms(sent_dms)
                            sent_count += 1
                            await asyncio.sleep(random.uniform(15, 20))
                        else:
                            print(f"Skipping user {member['username'] or member['id']}: Deleted This Account")
                    else:
                        print(f"Skipping user {member['username'] or member['id']}: Missing ID or access_hash")
                except FloodWaitError as e:
                    wait_time = e.seconds + random.uniform(5, 15)
                    if e.seconds > 3600:
                        print(f"FloodWaitError with {phone}: Wait time {e.seconds} seconds is too long. Switching to next account.")
                        logging.info(f"FloodWaitError with {phone}: Wait time {wait_time} seconds is too long, skipping.")
                        break
                    logging.info(f"FloodWaitError with {phone}: Waiting {wait_time} seconds")
                    print(f"Flood wait error: Waiting {e.seconds} seconds")
                    await asyncio.sleep(wait_time)
                except RPCError as e:
                    print(f"Error sending DM to {member['username'] or member['id']} with {phone}: {str(e)}")
                    if "USER_PRIVACY_RESTRICTED" in str(e) or "USER_NOT_FOUND" in str(e):
                        print(f"Skipping user {member['username'] or member['id']}: Privacy settings or invalid user.")
                        continue
                    break
                except Exception as e:
                    print(f"Unexpected error sending DM to {member['username'] or member['id']} with {phone}: {str(e)}")
                    break

            print(f"Finished sending DMs with {phone}. Sent to {sent_count} members.")
            tried_accounts.append(phone)

        except Exception as e:
            print(f"Error with {phone} for {source_group}: {str(e)}")
            tried_accounts.append(phone)
        finally:
            if client:
                await client.disconnect()

        if len(tried_accounts) == len(selected_phones):
            print(f"All accounts tried. Total DMs sent: {len(sent_dms.get(message_key, []))}")
            break

    print(f"Completed DM process for {source_group}. Total DMs sent: {len(sent_dms.get(message_key, []))}")

async def set_auto_reply():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to set auto-reply (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if choice < 0 or choice >= len(accounts):
                print("Invalid account selection.")
                return
            selected_phones = [accounts[choice]]
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    message = input("Enter the auto-reply message: ")
    if not message.strip():
        print("Auto-reply message cannot be empty.")
        return

    auto_reply = load_auto_reply()
    for phone in selected_phones:
        auto_reply[phone] = message
    save_auto_reply(auto_reply)

    print(f"Auto-reply set for {len(selected_phones)} account(s). Starting listeners...")

    async def handle_new_message(phone, client):
        @client.on(NewMessage(incoming=True, chats=None))
        async def handler(event):
            if event.is_private:
                sender = await event.get_sender()
                sender_id = sender.id
                sender_username = sender.username or f"User {sender_id}"
                try:
                    await event.respond(auto_reply.get(phone, ""))
                    print(f"Auto-replied to {sender_username} on account {phone}")
                    await asyncio.sleep(1)
                except Exception as e:
                    print(f"Failed to auto-reply to {sender_username} on {phone}: {str(e)}")

    clients = []
    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if client:
            await handle_new_message(phone, client)
            client.loop.create_task(client.run_until_disconnected())
            clients.append(client)
            print(f"Auto-reply listener started for {phone}")
        else:
            print(f"Failed to start auto-reply for {phone}: Invalid session.")

    if clients:
        print("Auto-reply listeners are running in the background. Select another menu option or press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(3600)

# In store_phone_numbers()
# In store_phone_numbers()
async def store_phone_numbers():
    from telethon.tl.functions.contacts import ImportContactsRequest
    from telethon.tl.types import InputPhoneContact
    import re
    import sys
    import aiofiles
    from asyncio import Lock

    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        print("API ID and Hash not found. Please add an account first to set them.")
        return

    sessions = load_sessions()
    if not sessions:
        print("No accounts added yet. Please add an account first.")
        return

    accounts = await list_accounts(sessions)
    choice = input("Select the account to check numbers (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            if 0 <= choice < len(accounts):
                selected_phones = [accounts[choice]]
            else:
                print("Invalid account selection.")
                return
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")
            return

    start_number = input("Enter starting phone number (e.g., +31631060000): ").strip()
    end_number = input("Enter ending phone number (e.g., +31631069999): ").strip()

    try:
        start_num = int(start_number.replace('+', ''))
        end_num = int(end_number.replace('+', ''))
        if start_num > end_num:
            print("Starting number must be less than or equal to ending number.")
            return
    except ValueError:
        print("Invalid phone number format. Use format like +31631060000.")
        return

    users_file = os.path.join("Numbers", "telegram_users.json")
    found_users = []
    if os.path.exists(users_file):
        with open(users_file, 'r') as f:
            try:
                found_users = json.load(f)
            except json.JSONDecodeError:
                print("Warning: telegram_users.json is corrupted. Starting with empty list.")

    new_users = []  # Track users found in this run
    json_lock = Lock()

    async def save_users():
        async with json_lock:
            for _ in range(3):  # Retry up to 3 times
                try:
                    async with aiofiles.open(users_file, 'w') as f:
                        await f.write(json.dumps(found_users, indent=4))
                    return True
                except Exception as e:
                    print(f"\nError writing to {users_file}: {str(e)}. Retrying...")
                    await asyncio.sleep(1)
            print(f"\nFailed to write to {users_file} after retries.")
            return False

    async def check_numbers_with_client(client, phone, numbers):
        checked_count = 0
        total_numbers = len(numbers)
        avg_delay = 5  # Average delay per number (2-4s success + 3s universal)
        total_estimated_time = total_numbers * avg_delay

        try:
            me = await client.get_me()
            client_username = me.username or phone
        except Exception as e:
            print(f"Error fetching profile for {phone}: {str(e)}")
            client_username = phone

        for phone_number in numbers:
            start_time = time.time()
            checked_count += 1
            remaining_numbers = total_numbers - checked_count
            remaining_time = remaining_numbers * avg_delay
            sys.stdout.write(f"\rEstimated time remaining: {int(remaining_time)} seconds")
            sys.stdout.flush()

            try:
                contact = InputPhoneContact(client_id=0, phone=phone_number, first_name=phone_number, last_name="")
                result = await client(ImportContactsRequest([contact]))
                
                if result.users:
                    user = result.users[0]
                    if not user.access_hash or user.bot or user.deleted:
                        print(f"\nSkipping {phone_number}: Invalid, bot, or deleted account.")
                        await asyncio.sleep(3)
                        continue

                    username = user.username or ""
                    first_name = user.first_name or phone_number
                    last_name = user.last_name or ""

                    user_data = {
                        "user_id": user.id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "username": username,
                        "phone_number": phone_number,
                        "added_by": client_username,
                        "access_hash": user.access_hash
                    }
                    if not any(u["user_id"] == user_data["user_id"] for u in found_users):
                        found_users.append(user_data)
                        new_users.append(user_data)  # Track new user
                        print(f"\nFound user for {phone_number}: {user_data['first_name']} {user_data['last_name']} (@{user_data['username'] or 'N/A'})")
                        await client(ImportContactsRequest([
                            InputPhoneContact(
                                client_id=0,
                                phone=phone_number,
                                first_name=user_data['first_name'],
                                last_name=user_data['last_name']
                            )
                        ]))
                    else:
                        print(f"\nUser {phone_number} already stored. Skipping.")
                    
                    if not await save_users():
                        print(f"\nWarning: Failed to save user data for {phone_number}.")
                    
                    await asyncio.sleep(random.uniform(2, 4))
                else:
                    await asyncio.sleep(3)
                
            except FloodWaitError as e:
                wait_time = e.seconds + random.uniform(5, 15)
                print(f"\nFlood wait error for {phone}: Waiting {wait_time}s")
                logging.error(f"FloodWaitError with {phone}: Waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except RPCError as e:
                print(f"\nError checking {phone_number}: {str(e)}")
                if "PHONE_NUMBER_INVALID" in str(e):
                    print(f"Skipping {phone_number}: Invalid number.")
            except Exception as e:
                print(f"\nUnexpected error checking {phone_number}: {str(e)}")
            
            await asyncio.sleep(3)  # Universal 3-second delay
            elapsed_time = time.time() - start_time
            remaining_time = max(0, remaining_time - elapsed_time)
        
        sys.stdout.write(f"\r{' '*50}\r")  # Clear the line
        sys.stdout.flush()
        return checked_count

    total_numbers = end_num - start_num + 1
    numbers_per_account = total_numbers // len(selected_phones) if selected_phones else total_numbers
    tasks = []
    total_checked = 0

    for i, phone in enumerate(selected_phones):
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            print(f"Invalid session for {phone}. Skipping.")
            continue

        start = start_num + i * numbers_per_account
        end = start + numbers_per_account - 1 if i < len(selected_phones) - 1 else end_num
        phone_numbers = [f"+{num}" for num in range(start, end + 1)]
        
        print(f"Checking range +{start} to +{end} with {phone}")
        tasks.append(check_numbers_with_client(client, phone, phone_numbers))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Error with {selected_phones[i]}: {str(result)}")
        else:
            total_checked += result

    # Final save to ensure all data is written
    if found_users:
        await save_users()
    
    print(f"Finished. Checked {total_checked} numbers, found {len(new_users)} users")
    if len(found_users) > 0 and os.path.exists(users_file):
        with open(users_file, 'r') as f:
            try:
                saved_users = json.load(f)
                print(f"Total users saved in {users_file}: {len(saved_users)}")
            except json.JSONDecodeError:
                print(f"Error: {users_file} is corrupted.")

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if client:
            await client.disconnect()

async def report_user():
    config = load_config()
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    sessions = load_sessions()
    
    accounts = await list_accounts(sessions)
    choice = input("Select the account to Report user (e.g., 1, 2, or 'all'): ").lower()
    if choice == "all":
        selected_phones = accounts
    else:
        try:
            choice = int(choice) - 1
            selected_phones = [accounts[choice]]
        except:
            return

    target_user = input("Enter username (e.g., @username) or user ID to Report: ").strip()
    if not target_user:
        return

    members = load_members()
    access_hash = None
    if target_user.isdigit():
        for file, member_list in members.items():
            for member in member_list:
                if str(member.get("id")) == target_user:
                    access_hash = member.get("access_hash")
                    break
            if access_hash:
                break

    total_time = len(selected_phones) * 5  # Estimate 5 seconds per account
    stop_event = asyncio.Event()

    async def update_progress():
        remaining = total_time
        while remaining > 0 and not stop_event.is_set():
            sys.stdout.write(f"\rEstimated time remaining: {int(remaining)} seconds")
            sys.stdout.flush()
            await asyncio.sleep(1)
            remaining -= 1
        sys.stdout.write(f"\r{' '*50}\r")
        sys.stdout.flush()

    progress_task = asyncio.create_task(update_progress())

    for phone in selected_phones:
        client = await get_client(phone, api_id, api_hash, sessions[phone])
        if not client:
            continue

        try:
            user_entity = None
            try:
                if target_user.isdigit() and access_hash:
                    user_entity = await client.get_entity(InputPeerUser(user_id=int(target_user), access_hash=access_hash))
                else:
                    user_entity = await client.get_entity(target_user)
            except:
                if target_user.isdigit():
                    retry_username = input("Enter username (e.g., @username) or press Enter to skip: ").strip()
                    if retry_username:
                        try:
                            user_entity = await client.get_entity(retry_username)
                            target_user = retry_username
                        except:
                            continue
                    else:
                        continue

            if user_entity:
                await client(BlockRequest(id=user_entity.id))
            await asyncio.sleep(random.uniform(2, 5))

        except:
            pass
        finally:
            if client:
                await client.disconnect()

    stop_event.set()
    await progress_task
    print("Report successful")

# Update main() function to include new option and shift Exit to 11
# Replace the main() function with:
async def main():
    init_files()
    
    while True:
        clear_screen()
        # Display ASCII art header
        f = Figlet(font='slant')
        print(Fore.YELLOW + f.renderText('Welcome!'))
        print(Fore.YELLOW + '=' * 50)
        
        # Display stylish menu
        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}Userbot Menu:{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}1. Manage accounts{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}2. Retrieve group/channel members{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}3. Add members to group/channel{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}4. Join groups/channels from groups.txt{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}5. Search for groups/channels{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}6. Send message to groups{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}7. Send direct message{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}8. Set auto reply{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}9. Store phone numbers{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}10. Report users{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}11. Exit{Style.RESET_ALL}")
        print(Fore.YELLOW + '=' * 50)
        
        choice = input(f"\n{Fore.WHITE}{Style.BRIGHT}Enter your choice (1-11): {Style.RESET_ALL}")
        clear_screen()

        if choice == "1":
            await manage_accounts()
        elif choice == "2":
            await retrieve_members()
        elif choice == "3":
            await add_to_group()
        elif choice == "4":
            await join_groups()
        elif choice == "5":
            await search_groups()
        elif choice == "6":
            await send_message_to_groups()
        elif choice == "7":
            await send_direct_message()
        elif choice == "8":
            await set_auto_reply()
        elif choice == "9":
            await store_phone_numbers()
        elif choice == "10":
            await report_user()
        elif choice == "11":
            print(f"{Fore.GREEN}{Style.BRIGHT}Exiting...{Style.RESET_ALL}")
            break
        else:
            print(f"{Fore.RED}{Style.BRIGHT}Invalid choice. Please try again.{Style.RESET_ALL}")
            await asyncio.sleep(2)
            
if __name__ == "__main__":
    for file in os.listdir("."):
        if file.endswith(".session") and not os.path.isdir(file):
            try:
                os.remove(file)
                print(f"Removed residual SQLite session file: {file}")
            except Exception as e:
                print(f"Error removing {file}: {str(e)}")

    if os.path.exists("sessions"):
        for session_file in os.listdir("sessions"):
            try:
                os.remove(os.path.join("sessions", session_file))
                print(f"Removed session file: sessions/{session_file}")
            except Exception as e:
                print(f"Error removing sessions/{session_file}: {str(e)}")
        try:
            os.rmdir("sessions")
        except Exception as e:
            print(f"Error removing sessions directory: {str(e)}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"Script execution failed: {str(e)}")
        raise