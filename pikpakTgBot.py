import json
import logging
import os
import re
import sys
import threading
import uuid
from time import sleep, time
from pikpakapi import PikPakApi
import asyncio
import requests
import telegram
from telegram import Update
from telegram.ext import Updater, CallbackContext, CommandHandler, Handler, MessageHandler, Filters
from flask import Flask, request, render_template, jsonify

from config import *

# 配置 Flask
app = Flask(__name__)
# 用來存儲最新的日誌訊息，供 Web UI 顯示
log_buffer = []
MAX_LOG_SIZE = 100

class ListBuffer(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        log_buffer.append(log_entry)
        if len(log_buffer) > MAX_LOG_SIZE:
            log_buffer.pop(0)

# 設置日誌
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 添加自定義 Handler 到 log_buffer
buffer_handler = ListBuffer()
buffer_handler.setFormatter(formatter)
logger.addHandler(buffer_handler)

# 也可以保留控制台輸出
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# 全局变量
SCHEMA = 'https' if ARIA2_HTTPS else 'http'
PIKPAK_API_URL = "https://api-drive.mypikpak.com"
PIKPAK_USER_URL = "https://user.mypikpak.com"

# 记录登陆账号的headers，调用api用
pikpak_headers = [None] * len(USER)
pikpak_clients = [None] * len(USER)
# 命令运行标志，防止下载与删除命令同时运行
running = False
# 记录下载线程
thread_list = []
# 记录待下载的磁力链接
mag_urls = []
# 登录锁
login_lock = threading.Lock()
# 批量任務鎖
batch_lock = threading.Lock()
# 批量任務狀態
batch_results = {}

# PTB所需
if TG_API_URL[-1] == '/':
    updater = Updater(token=TOKEN, base_url=f"{TG_API_URL}bot", base_file_url=f"{TG_API_URL}file/bot")
else:
    updater = Updater(token=TOKEN, base_url=f"{TG_API_URL}/bot", base_file_url=f"{TG_API_URL}/file/bot")

dispatcher = updater.dispatcher

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/add', methods=['POST'])
def api_add():
    data = request.json
    content = data.get('magnets', '')
    
    # 簡單的正則提取磁力鏈接
    magnets = re.findall(r'magnet:\?xt=urn:btih:[0-9a-fA-F]{40,}.*', content)
    
    if not magnets:
        return jsonify({'status': 'error', 'message': '未找到有效的磁力連結'}), 400

    # 模擬 TG update 對象，讓 main 函數可以運作
    # 注意：這裡我們使用一個假的 update 對象，只為了兼容 main 函數的參數
    # 因為 main 函數會用到 update.effective_chat.id 來發送通知
    # 我們這裡取 ADMIN_IDS[0] 作為通知對象
    
    class MockChat:
        id = ADMIN_IDS[0]
        
    class MockUpdate:
        effective_chat = MockChat()
        
    mock_update = MockUpdate()
    
    # 初始化批量任務追蹤
    batch_id = str(uuid.uuid4())[:8]
    with batch_lock:
            batch_results[batch_id] = {
                'total': len(magnets),
                'processed': 0,
                'results': []
            }
            
    logging.info(f"Web UI 收到 {len(magnets)} 個磁力下載請求")

    # 通知 Telegram
    try:
        msg = f"📥 收到來自 Web UI 的 {len(magnets)} 個下載任務：\n\n"
        for i, mag in enumerate(magnets, 1):
            # 簡化連結顯示，只取 xt 部分
            mag_url_part = re.search(r'xt=.+?(&|$)', mag)
            mag_simple = mag_url_part.group(0).rstrip('&') if mag_url_part else mag[:40] + "..."
            msg += f"{i}. <code>{mag_simple}</code>\n"
        
        updater.bot.send_message(chat_id=ADMIN_IDS[0], text=msg, parse_mode='HTML')
    except Exception as e:
        logging.error(f"Web UI 通知發送失敗: {e}")

    # 啟動下載線程
    global PIKPAK_OFFLINE_PATH
    offline_path = None
    if str(PIKPAK_OFFLINE_PATH) not in ["None", "/My Pack"]:
        offline_path = PIKPAK_OFFLINE_PATH

    for magnet in magnets:
        thread_list.append(threading.Thread(target=main, args=[mock_update, None, magnet, offline_path, batch_id]))
        thread_list[-1].start()
        # 增加延遲，避免同時發起過多請求導致 PikPak 報錯 (HTTP 400 operation too frequent)
        sleep(2)

    return jsonify({'status': 'ok', 'count': len(magnets)})

@app.route('/api/logs')
def api_logs():
    return jsonify({'logs': log_buffer})

def call_aria2(method, params=None):
    """Helper to call Aria2 JSON-RPC"""
    if params is None:
        params = []
    
    payload = {
        'jsonrpc': '2.0',
        'id': 'webui',
        'method': method,
        'params': [f"token:{ARIA2_SECRET}"] + params
    }
    try:
        response = requests.post(f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc', 
                               json=payload, timeout=2).json()
        return response.get('result', [])
    except Exception as e:
        return []

@app.route('/api/stats')
def api_stats():
    tasks = []
    
    # 1. 獲取 PikPak 離線任務 (僅獲取第一個帳號，避免請求過多)
    try:
        if USER:
            # 注意：這裡假設 get_offline_list 可以直接被調用，且 USER[0] 是有效的
            # 為了避免頻繁調用導致被封，這裡最好能加個緩存，但為了簡單先直接調用
            # 如果 get_offline_list 失敗，不會影響 Aria2 的顯示
            pikpak_tasks = get_offline_list(USER[0])
            for task in pikpak_tasks:
                # PikPak 的 status: PHASE_TYPE_RUNNING, PHASE_TYPE_COMPLETE, PHASE_TYPE_ERROR
                phase = task.get('phase')
                progress = int(task.get('progress', 0))
                error_msg = task.get('message', '')

                # 1. 忽略離線完成且進度 100% 的任務，避免 Dashboard 過於擁擠
                if phase == 'PHASE_TYPE_COMPLETE' and progress == 100:
                    continue
                
                # 2. 忽略 "file deleted" 錯誤 (正常情況)
                if phase == 'PHASE_TYPE_ERROR':
                    if "file deleted" in error_msg.lower() or "file_deleted" in error_msg.lower():
                        continue
                    status = 'cloud_error'
                else:
                    # 顯示正在離線下載中或其他非錯誤狀態
                    status = 'cloud_downloading'
                
                tasks.append({
                    'type': 'pikpak',
                    'gid': task.get('id'),
                    'name': task.get('name') or task.get('file_name') or 'Unknown',
                    'status': status,
                    'total': int(task.get('file_size', 0)),
                    'completed': int(task.get('file_size', 0)) * progress // 100,
                    'speed': 0, # PikPak API 通常不返回即時速度
                    'progress': progress,
                    'error': error_msg if phase == 'PHASE_TYPE_ERROR' else ''
                })
    except Exception as e:
        # logging.error(f"PikPak Stats Error: {e}") # 降低日誌級別或暫時忽略，避免刷屏
        pass

    # 2. 獲取 Aria2 任務
    keys = ["gid", "status", "files", "totalLength", "completedLength", "downloadSpeed", "errorMessage"]
    try:
        active = call_aria2('aria2.tellActive', [keys])
        waiting = call_aria2('aria2.tellWaiting', [0, 100, keys])
        all_aria_tasks = active + waiting
        
        for task in all_aria_tasks:
            name = "Unknown"
            if task.get('files') and len(task['files']) > 0:
                path = task['files'][0].get('path', '')
                if path:
                    name = os.path.basename(path)
                else:
                    uris = task['files'][0].get('uris', [])
                    if uris:
                        name = uris[0].get('uri', 'Unknown')

            total = int(task.get('totalLength', 0))
            completed = int(task.get('completedLength', 0))
            progress = int(completed / total * 100) if total > 0 else 0

            tasks.append({
                'type': 'aria2',
                'gid': task.get('gid'),
                'name': name,
                'status': task.get('status'), # active, waiting, paused, error, complete
                'total': total,
                'completed': completed,
                'speed': int(task.get('downloadSpeed', 0)),
                'progress': progress,
                'error': task.get('errorMessage', '')
            })
    except Exception as e:
        logging.error(f"Aria2 Stats Error: {e}")

    return jsonify({'tasks': tasks})

@app.route('/api/stuck')
def api_stuck():
    """獲取卡住的任務列表"""
    min_progress = request.args.get('min_progress', 90, type=int)
    
    all_stuck = []
    for account in USER:
        stuck = get_stuck_tasks(account, min_progress)
        for task in stuck:
            task['account'] = account
            all_stuck.append(task)
    
    return jsonify({'tasks': all_stuck, 'count': len(all_stuck)})

@app.route('/api/retry', methods=['POST'])
def api_retry():
    """重試卡住的任務"""
    data = request.json or {}
    min_progress = data.get('min_progress', 90)
    delete_cloud = data.get('delete_cloud', True)
    
    logging.info(f"Web UI 觸發重試卡住任務 (進度 >= {min_progress}%)")
    
    total_success = 0
    total_fail = 0
    all_results = []
    
    for account in USER:
        success, fail, results = retry_stuck_tasks(account, min_progress, delete_cloud_files=delete_cloud)
        total_success += success
        total_fail += fail
        if results:
            for r in results:
                r['account'] = account
            all_results.extend(results)
    
    # 通知 Telegram
    if total_success + total_fail > 0:
        try:
            msg = f"🔄 Web UI 觸發重試卡住任務\n"
            msg += f"✅ 成功: {total_success}\n"
            msg += f"❌ 失敗: {total_fail}"
            updater.bot.send_message(chat_id=ADMIN_IDS[0], text=msg)
        except Exception as e:
            logging.error(f"通知發送失敗: {e}")
    
    return jsonify({
        'status': 'ok',
        'success': total_success,
        'fail': total_fail,
        'results': all_results
    })

@app.route('/api/clean', methods=['POST'])
def api_clean():
    """清理雲端檔案和離線任務記錄"""
    data = request.json or {}
    mode = data.get('mode', 'all')  # all, deep, tasks, tasks_error
    
    logging.info(f"Web UI 觸發清理 (模式: {mode})")
    
    results = []
    
    for account in USER:
        login(account)
        account_result = {'account': account, 'actions': []}
        
        if mode == 'deep':
            # 深度清理：檔案 + 回收站 + 所有離線任務記錄
            all_file_id = list(get_folder_all(account))
            if len(all_file_id) > 0:
                delete_files(all_file_id, account, mode='all')
                account_result['actions'].append(f"已刪除 {len(all_file_id)} 個檔案")
            
            if empty_trash(account):
                account_result['actions'].append("回收站已清空")
            
            success, fail = delete_offline_tasks(account)
            if success > 0:
                account_result['actions'].append(f"已清理 {success} 個離線任務記錄")
                
        elif mode == 'tasks':
            # 只清理所有離線任務記錄
            success, fail = delete_offline_tasks(account)
            if success > 0:
                account_result['actions'].append(f"已清理 {success} 個離線任務記錄")
                
        elif mode == 'tasks_error':
            # 只清理失敗的離線任務記錄
            success, fail = delete_offline_tasks(account, phase_filter='PHASE_TYPE_ERROR')
            if success > 0:
                account_result['actions'].append(f"已清理 {success} 個失敗的離線任務記錄")
                
        else:  # all
            # 標準清理：檔案 + 失敗的離線任務記錄
            all_file_id = list(get_folder_all(account))
            if len(all_file_id) > 0:
                delete_files(all_file_id, account, mode='all')
                delete_trash(all_file_id, account, mode='all')
                account_result['actions'].append(f"已刪除 {len(all_file_id)} 個檔案")
            
            success, fail = delete_offline_tasks(account, phase_filter='PHASE_TYPE_ERROR')
            if success > 0:
                account_result['actions'].append(f"已清理 {success} 個失敗的離線任務記錄")
        
        if not account_result['actions']:
            account_result['actions'].append("無需清理")
        
        results.append(account_result)
    
    # 通知 Telegram
    try:
        msg = f"🧹 Web UI 觸發清理 (模式: {mode})\n"
        for r in results:
            msg += f"\n{r['account'].split('@')[0]}:\n"
            for action in r['actions']:
                msg += f"  ✅ {action}\n"
        updater.bot.send_message(chat_id=ADMIN_IDS[0], text=msg)
    except Exception as e:
        logging.error(f"通知發送失敗: {e}")
    
    return jsonify({'status': 'ok', 'results': results})

def run_flask():
    # 關閉 Flask 的啟動 banner
    cli = sys.modules['flask.cli']
    cli.show_server_banner = lambda *x: None
    
    # 隱藏 Werkzeug 的訪問日誌
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    # 運行在 0.0.0.0 讓外部可訪問
    port = int(globals().get('WEB_PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


# 用户限制：Stack Overflow 用户@Majid提供的方法
# from: https://stackoverflow.com/questions/62466399/how-can-i-restrict-a-telegram-bots-use-to-some-users-only#answers-header
class AdminHandler(Handler):
    def __init__(self):
        super().__init__(self.cb)

    def cb(self, update: telegram.Update, context):
        update.message.reply_text('Unauthorized access')

    def check_update(self, update: telegram.update.Update):
        if update.message is None or str(update.message.from_user.id) not in ADMIN_IDS:
            return True

        return False


def registerFuc():
    try:
        url = 'https://pikpak.kinh.cc/GetFreeAccount.php'
        resp = requests.get(url)
        account = resp.json()['Data'].split('|')[0].split(':')[1].strip()
        password = resp.json()['Data'].split('|')[1].split(':')[1].strip()
        if account and password:
            return {'account': account, 'password': password}
        else:
            return False
    except Exception as e:
        logging.error(e)
        return False


def auto_delete_judge(account):
    try:
        status = AUTO_DELETE[account]
        if status.upper() == 'TRUE':
            return 'on'
        else:
            return 'off'
    except Exception as e:
        logging.error(f"{e}未配置，默認開啟自動刪除")
        return 'on'


def start(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="【指令簡介】\n" 
                                  "/p\t自動離線+aria2下載+釋放雲端硬碟空間\n" 
                                  "/account\t管理帳號（發送/account查看使用說明）\n" 
                                  "/clean\t清空雲端硬碟+離線任務記錄（發送/clean查看使用說明）\n" 
                                  "/path\t管理pikpak離線下載的路徑\n"
                                  "/retry\t重試卡住的離線任務（發送/retry查看使用說明）\n")


# 账号密码登录
def login(account):
    with login_lock:
        index = USER.index(account)

        # 登录所需所有信息
        login_admin = account
        login_password = PASSWORD[index]

        client = PikPakApi(
            username=login_admin,
            password=login_password,
        )

        # 执行异步的登录和刷新操作，并等待完成
        # 使用新的事件循環以避免 "Event loop is closed" 錯誤
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(client.login())
            loop.run_until_complete(client.refresh_access_token())
        finally:
            loop.close()
        headers = client.get_headers()
        pikpak_headers[index] = headers.copy()  # 拷贝
        pikpak_clients[index] = client

        logging.info(f"帳號{account}登入成功！")


# 获得headers，用于请求api
def get_headers(account):
    index = USER.index(account)

    if not pikpak_headers[index]:  # headers为空则先登录
        login(account)
    return pikpak_headers[index]


def get_clients(account):
    index = USER.index(account)

    if not pikpak_clients[index]:  # clients为空则先登录
        login(account)
    return pikpak_clients[index]

# 离线下载磁力
def magnet_upload(file_url, account, parent_id=None, offline_path=None):
    # 请求离线下载所需数据
    login_headers = get_headers(account)
    client = get_clients(account)
    torrent_url = f"{PIKPAK_API_URL}/drive/v1/files"
    # 获取离线下载路径id
    if offline_path:
        parent_ids = asyncio.run(client.path_to_id(path=offline_path, create=True))
        if parent_ids and offline_path.split("/")[-1] == parent_ids[-1]["name"]:
            parent_id = parent_ids[-1]["id"]

    # 磁力下载
    torrent_data = {
        "kind": "drive#file",
        "name": "",
        "upload_type": "UPLOAD_TYPE_URL",
        "url": {"url": file_url},
        "folder_type": "DOWNLOAD" if not parent_id else "",
        "parent_id": parent_id,
    }
    # 请求离线下载
    torrent_result = requests.post(url=torrent_url, headers=login_headers, json=torrent_data, timeout=5).json()

    # 处理请求异常
    if "error" in torrent_result:
        if torrent_result['error_code'] == 16:
            logging.info(f"帳號{account}登入過期，正在重新登入")
            login(account)  # 重新登录该账号
            login_headers = get_headers(account)
            torrent_result = requests.post(url=torrent_url, headers=login_headers, json=torrent_data, timeout=5).json()

        else:
            # 可以考虑加入删除离线失败任务的逻辑
            logging.error(f"帳號{account}提交離線下載任務失敗，錯誤訊息：{torrent_result['error_description']}")
            return None, None

    # 输出日志
    file_url_part = re.search(r'^(magnet:\?).*(xt=.+?)(&|$)', file_url)
    if file_url_part:
        file_url_simple = ''.join(file_url_part.groups()[:-1])
        logging.info(f"帳號{account}添加離線任務:{file_url_simple}")
    else:
        logging.info(f"帳號{account}添加離線任務:{file_url}")

    # 返回离线任务id、下载文件名
    return torrent_result['task']['id'], torrent_result['task']['name']


# 获取所有离线任务
def get_offline_list(account):
    # 准备信息
    login_headers = get_headers(account)
    tasks = []
    next_page_token = ""

    while True:
        offline_list_url = f"{PIKPAK_API_URL}/drive/v1/tasks?type=offline&page_token={next_page_token}&thumbnail_size=SIZE_LARGE&filters=%7B%7D&with=reference_resource"
        # 发送请求
        offline_list_info = requests.get(url=offline_list_url, headers=login_headers, timeout=5).json()
        # 处理错误
        if "error" in offline_list_info:
            if offline_list_info['error_code'] == 16:
                logging.info(f"帳號{account}登入過期，正在重新登入")
                login(account)
                login_headers = get_headers(account)
                continue # Retry current page
            else:
                logging.error(f"帳號{account}獲取離線任務失敗，錯誤訊息：{offline_list_info.get('error_description')}")
                # Return whatever we have collected so far, or empty list if failed on first page
                return tasks

        tasks.extend(offline_list_info.get('tasks', []))
        
        next_page_token = offline_list_info.get('next_page_token', '')
        if not next_page_token:
            break

    return tasks


# 获取下载信息
def get_download_url(file_id, account):
    for tries in range(3):
        try:
            # 准备信息
            login_headers = get_headers(account)
            download_url = f"{PIKPAK_API_URL}/drive/v1/files/{file_id}?_magic=2021&thumbnail_size=SIZE_LARGE"
            # 发送请求
            download_info = requests.get(url=download_url, headers=login_headers, timeout=5).json()
            # logging.info('返回文件信息包括：\n' + str(download_info))

            # 处理错误
            if "error" in download_info:
                if download_info['error_code'] == 16:
                    logging.info(f"帳號{account}登入過期，正在重新登入")
                    login(account)
                    login_headers = get_headers(account)
                    # Retry immediately with new headers
                    download_info = requests.get(url=download_url, headers=login_headers, timeout=5).json()
                
                # Check error again after potential re-login
                if "error" in download_info:
                     logging.error(f"帳號{account}獲取檔案下載資訊失敗，錯誤訊息：{download_info['error_description']}")
                     sleep(2)
                     continue # Retry loop

            # 返回文件名、文件下载直链
            return download_info['name'], download_info['web_content_link']

        except Exception as e:
            logging.error(f'帳號{account}獲取檔案下載資訊失敗（第{tries+1}/3次）：{e}')
            sleep(2)
            continue
            
    return "", ""


# 获取文件夹下所有id
def get_list(folder_id, account):
    try:
        file_list = []
        # 准备信息
        login_headers = get_headers(account)
        list_url = f"{PIKPAK_API_URL}/drive/v1/files?parent_id={folder_id}&thumbnail_size=SIZE_LARGE" + \
                   "&filters=%7B%22trashed%22:%7B%22eq%22:false%7D%7D"
        # 发送请求
        list_result = requests.get(url=list_url, headers=login_headers, timeout=5).json()
        # 处理错误
        if "error" in list_result:
            if list_result['error_code'] == 16:
                logging.info(f"帳號{account}登入過期，正在重新登入")
                login(account)
                login_headers = get_headers(account)
                list_result = requests.get(url=list_url, headers=login_headers, timeout=5).json()
            else:
                logging.error(f"帳號{account}獲取資料夾下檔案id失敗，錯誤訊息：{list_result['error_description']}")
                return file_list

        file_list += list_result['files']

        # 获取下一页
        while list_result['next_page_token'] != "":
            list_url = f"{PIKPAK_API_URL}/drive/v1/files?parent_id={folder_id}&page_token=" + list_result[
                'next_page_token'] + \
                       "&thumbnail_size=SIZE_LARGE" + "&filters=%7B%22trashed%22:%7B%22eq%22:false%7D%7D "

            list_result = requests.get(url=list_url, headers=login_headers, timeout=5).json()

            file_list += list_result['files']

        # logging.info(file_list)
        return file_list

    except Exception as e:
        logging.error(f"帳號{account}獲取資料夾下檔案id失敗:{e}")
        return []


# 获取文件夹及其子目录下所有文件id
def get_folder_all_file(folder_id, path, account):
    # 获取该文件夹下所有id
    folder_list = get_list(folder_id, account)
    # 逐个判断每个id
    for a in folder_list:
        # 如果是文件
        if a["kind"] == "drive#file":
            down_name, down_url = get_download_url(a["id"], account)
            if down_name == "":
                continue
            yield down_name, down_url, a['id'], path  # 文件名、下载直链、文件id、文件路径
        # 如果是根目录且文件夹是My Pack，则不更新path
        elif a['name'] == 'My Pack' and folder_id == '':
            yield from get_folder_all_file(a["id"], path, account)
        # 其他文件夹
        else:
            new_path = path + a['name'] + "/"
            yield from get_folder_all_file(a["id"], new_path, account)


# 获取根目录文件夹下所有文件、文件夹id，清空网盘时用
def get_folder_all(account):
    # 获取根目录文件夹下所有id
    folder_list = get_list('', account)
    # 逐个判断每个id
    for a in folder_list:
        # 是文件则直接返回id
        if a["kind"] == "drive#file":
            yield a['id']
        # My Pack文件夹则获取其下所有id
        elif a["name"] == 'My Pack':
            for b in get_list(a['id'], account):
                yield b['id']
        # 其他文件夹也直接返回id
        else:
            yield a['id']


# 删除文件夹、文件
def delete_files(file_id, account, mode='normal'):
    # 判断是否开启自动清理
    if mode == 'normal':
        if auto_delete_judge(account) == 'off':
            logging.info('帳號{}未開啟自動清理'.format(account))
            return False
        else:
            logging.info('帳號{}開啟了自動清理'.format(account))
    # 准备数据
    login_headers = get_headers(account)
    delete_files_url = f"{PIKPAK_API_URL}/drive/v1/files:batchTrash"
    if type(file_id) == list:  # 可以删除多个id
        delete_files_data = {"ids": file_id}
    else:
        delete_files_data = {"ids": [file_id]}
    # 发送请求
    delete_files_result = requests.post(url=delete_files_url, headers=login_headers, json=delete_files_data,
                                        timeout=5).json()
    # 处理错误
    if "error" in delete_files_result:
        if delete_files_result['error_code'] == 16:
            logging.info(f"帳號{account}登入過期，正在重新登入")
            login(account)
            login_headers = get_headers(account)
            delete_files_result = requests.post(url=delete_files_url, headers=login_headers, json=delete_files_data,
                                                timeout=5).json()

        else:
            logging.error(f"帳號{account}刪除雲端硬碟檔案失敗，錯誤訊息：{delete_files_result['error_description']}")
            return False

    return True


# 删除回收站id
def delete_trash(file_id, account, mode='normal'):
    # 判断是否开启自动清理
    if mode == 'normal':
        if auto_delete_judge(account) == 'off':
            logging.info('帳號{}未開啟自動清理'.format(account))
            return False
        else:
            logging.info('帳號{}開啟了自動清理'.format(account))
    # 准备信息
    login_headers = get_headers(account)
    delete_files_url = f"{PIKPAK_API_URL}/drive/v1/files:batchDelete"
    if type(file_id) == list:  # 可以删除多个id
        delete_files_data = {"ids": file_id}
    else:
        delete_files_data = {"ids": [file_id]}
    # 发送请求
    delete_files_result = requests.post(url=delete_files_url, headers=login_headers, json=delete_files_data,
                                        timeout=5).json()
    # 处理错误
    if "error" in delete_files_result:
        if delete_files_result['error_code'] == 16:
            logging.info(f"帳號{account}登入過期，正在重新登入")
            login(account)
            login_headers = get_headers(account)
            delete_files_result = requests.post(url=delete_files_url, headers=login_headers, json=delete_files_data,
                                                timeout=5).json()
        else:
            logging.error(f"帳號{account}刪除垃圾桶檔案失敗，錯誤訊息：{delete_files_result['error_description']}")
            return False

    return True


# 刪除離線任務記錄 (不是刪除檔案，是刪除任務列表中的記錄)
def delete_offline_tasks(account, task_ids=None, delete_files_too=False, phase_filter=None):
    """
    刪除離線任務記錄
    account: 帳號
    task_ids: 指定要刪除的任務 ID 列表，如果為 None 則根據 phase_filter 刪除
    delete_files_too: 是否同時刪除雲端檔案
    phase_filter: 篩選特定狀態的任務 (如 'PHASE_TYPE_ERROR')，None 表示全部
    
    返回: (success_count, fail_count)
    """
    login_headers = get_headers(account)
    
    # 如果沒有指定 task_ids，則獲取所有任務
    if task_ids is None:
        tasks = get_offline_list(account)
        if phase_filter:
            task_ids = [t['id'] for t in tasks if t.get('phase') == phase_filter]
        else:
            task_ids = [t['id'] for t in tasks]
    
    if not task_ids:
        logging.info(f"帳號{account}沒有需要刪除的離線任務記錄")
        return 0, 0
    
    logging.info(f"帳號{account}準備刪除 {len(task_ids)} 個離線任務記錄")
    
    # PikPak API 限制每次最多刪除 100 個任務，需要分批
    success_count = 0
    fail_count = 0
    batch_size = 50
    
    for i in range(0, len(task_ids), batch_size):
        batch = task_ids[i:i + batch_size]
        
        delete_url = f"{PIKPAK_API_URL}/drive/v1/tasks"
        params = {
            "task_ids": ",".join(batch),
            "delete_files": "true" if delete_files_too else "false",
        }
        
        try:
            result = requests.delete(url=delete_url, headers=login_headers, params=params, timeout=15)
            
            if result.status_code == 200:
                success_count += len(batch)
                logging.info(f"帳號{account}成功刪除 {len(batch)} 個離線任務記錄")
            else:
                # 嘗試重新登入
                if result.status_code == 401 or 'error_code' in result.text:
                    logging.info(f"帳號{account}登入過期，正在重新登入")
                    login(account)
                    login_headers = get_headers(account)
                    result = requests.delete(url=delete_url, headers=login_headers, params=params, timeout=15)
                    if result.status_code == 200:
                        success_count += len(batch)
                        logging.info(f"帳號{account}重試後成功刪除 {len(batch)} 個離線任務記錄")
                    else:
                        fail_count += len(batch)
                        logging.error(f"帳號{account}刪除離線任務記錄失敗: {result.text}")
                else:
                    fail_count += len(batch)
                    logging.error(f"帳號{account}刪除離線任務記錄失敗: {result.text}")
        except Exception as e:
            fail_count += len(batch)
            logging.error(f"帳號{account}刪除離線任務記錄時發生錯誤: {e}")
        
        sleep(1)  # 避免請求過於頻繁
    
    logging.info(f"帳號{account}離線任務記錄清理完成: 成功 {success_count}, 失敗 {fail_count}")
    return success_count, fail_count


# 清空回收站 (徹底清空，不是刪除指定檔案)
def empty_trash(account):
    """
    清空回收站中的所有檔案
    """
    login_headers = get_headers(account)
    empty_url = f"{PIKPAK_API_URL}/drive/v1/files/trash:empty"
    
    try:
        result = requests.post(url=empty_url, headers=login_headers, json={}, timeout=15)
        
        if result.status_code == 200:
            logging.info(f"帳號{account}回收站已清空")
            return True
        else:
            if 'error_code' in result.text:
                login(account)
                login_headers = get_headers(account)
                result = requests.post(url=empty_url, headers=login_headers, json={}, timeout=15)
                if result.status_code == 200:
                    logging.info(f"帳號{account}回收站已清空")
                    return True
            logging.error(f"帳號{account}清空回收站失敗: {result.text}")
            return False
    except Exception as e:
        logging.error(f"帳號{account}清空回收站時發生錯誤: {e}")
        return False


# 重試離線下載任務 (使用 PikPak API 的 retry 功能)
def retry_offline_task(task_id, account):
    """
    使用 PikPak 的 RETRY 功能重新開始離線任務
    這會讓 PikPak 重新嘗試下載，不需要原始 magnet link
    """
    login_headers = get_headers(account)
    retry_url = f"{PIKPAK_API_URL}/drive/v1/task"
    retry_data = {
        "type": "offline",
        "create_type": "RETRY",
        "id": task_id,
    }
    
    try:
        result = requests.post(url=retry_url, headers=login_headers, json=retry_data, timeout=10).json()
        
        if "error" in result:
            if result['error_code'] == 16:
                logging.info(f"帳號{account}登入過期，正在重新登入")
                login(account)
                login_headers = get_headers(account)
                result = requests.post(url=retry_url, headers=login_headers, json=retry_data, timeout=10).json()
            else:
                logging.error(f"帳號{account}重試任務失敗: {result.get('error_description', result)}")
                return False, result.get('error_description', 'Unknown error')
        
        logging.info(f"帳號{account}成功重試任務 {task_id}")
        return True, result
    except Exception as e:
        logging.error(f"帳號{account}重試任務時發生錯誤: {e}")
        return False, str(e)


# 刪除離線任務 (可選擇是否同時刪除雲端檔案)
def delete_offline_task(task_ids, account, delete_files=False):
    """
    刪除離線任務
    task_ids: 單個 task_id 或 list of task_ids
    delete_files: 是否同時刪除雲端檔案
    """
    login_headers = get_headers(account)
    delete_url = f"{PIKPAK_API_URL}/drive/v1/tasks"
    
    if isinstance(task_ids, str):
        task_ids = [task_ids]
    
    params = {
        "task_ids": ",".join(task_ids),
        "delete_files": "true" if delete_files else "false",
    }
    
    try:
        result = requests.delete(url=delete_url, headers=login_headers, params=params, timeout=10)
        
        if result.status_code == 200:
            logging.info(f"帳號{account}成功刪除 {len(task_ids)} 個任務")
            return True, None
        else:
            error_msg = result.text
            logging.error(f"帳號{account}刪除任務失敗: {error_msg}")
            return False, error_msg
    except Exception as e:
        logging.error(f"帳號{account}刪除任務時發生錯誤: {e}")
        return False, str(e)


# 獲取卡住的任務 (進度達到指定值但未完成)
def get_stuck_tasks(account, min_progress=90):
    """
    獲取卡住的離線任務
    min_progress: 最小進度閾值，預設 90%
    返回: [{id, name, progress, file_id}, ...]
    """
    tasks = get_offline_list(account)
    stuck = []
    
    logging.debug(f"帳號{account}共有 {len(tasks)} 個離線任務，篩選進度 >= {min_progress}%")
    
    for task in tasks:
        phase = task.get('phase', '')
        progress = int(task.get('progress', 0))
        message = task.get('message', '')
        name = task.get('name') or task.get('file_name') or 'Unknown'
        
        logging.debug(f"  任務: {name}, phase={phase}, progress={progress}%")
        
        # 忽略已完成的 (phase=COMPLETE 且 progress=100)
        if phase == 'PHASE_TYPE_COMPLETE' and progress == 100:
            continue
        
        # 忽略已刪除的檔案
        if "file deleted" in message.lower() or "file_deleted" in message.lower():
            continue
        
        # 忽略錯誤狀態（非卡住，是真的失敗）
        if phase == 'PHASE_TYPE_ERROR':
            continue
        
        # 篩選卡住的任務: 進度 >= min_progress 但尚未完成
        # 放寬條件：不限定 phase，只要進度夠高但沒完成就算卡住
        if progress >= min_progress and progress < 100:
            stuck.append({
                'id': task.get('id'),
                'name': name,
                'progress': progress,
                'file_id': task.get('file_id'),
                'phase': phase,  # 加入 phase 供 debug
            })
            logging.debug(f"    ↳ 判定為卡住的任務")
    
    return stuck


# 重試卡住的任務
def retry_stuck_tasks(account, min_progress=90, delete_cloud_files=True):
    """
    找出並重試卡住的任務
    1. 找出進度 >= min_progress 但未完成的任務
    2. 刪除這些任務的雲端檔案 (可選)
    3. 使用 PikPak 的 RETRY 功能重新開始
    
    返回: (success_count, fail_count, results)
    """
    stuck_tasks = get_stuck_tasks(account, min_progress)
    
    if not stuck_tasks:
        logging.info(f"帳號{account}沒有找到卡住的任務 (進度 >= {min_progress}%)")
        return 0, 0, []
    
    logging.info(f"🔄 帳號{account}找到 {len(stuck_tasks)} 個卡住的任務 (進度 >= {min_progress}%)")
    
    results = []
    success_count = 0
    fail_count = 0
    total = len(stuck_tasks)
    
    for i, task in enumerate(stuck_tasks, 1):
        task_id = task['id']
        task_name = task['name']
        file_id = task.get('file_id')
        progress = task['progress']
        
        logging.info(f"[{i}/{total}] 正在處理: {task_name} ({progress}%)")
        
        # Step 1: 刪除雲端檔案 (如果有且啟用)
        if delete_cloud_files and file_id:
            try:
                delete_files(file_id, account, mode='force')
                delete_trash(file_id, account, mode='force')
                logging.info(f"  ↳ 已刪除雲端不完整檔案")
            except Exception as e:
                logging.warning(f"  ↳ 刪除雲端檔案失敗 (繼續重試): {e}")
        
        # Step 2: 使用 PikPak retry
        success, result = retry_offline_task(task_id, account)
        
        if success:
            success_count += 1
            logging.info(f"  ↳ ✅ 已重新加入佇列")
            
            # Step 3: 啟動監控線程，等待 PikPak 完成後推送 Aria2
            # 使用 main() 的 resume_task 模式
            new_task_id = result.get('task', {}).get('id') if isinstance(result, dict) else None
            task_info = {
                'id': new_task_id or task_id,  # 優先使用新的 task_id
                'name': task_name
            }
            thread_list.append(threading.Thread(
                target=main,
                args=[None, None, None, None, None, task_info, account]
            ))
            thread_list[-1].start()
            logging.info(f"  ↳ 已啟動監控線程，等待完成後將推送 Aria2")
            
            results.append({
                'name': task_name,
                'progress': progress,
                'status': 'success',
                'message': '已重新加入佇列並啟動監控'
            })
        else:
            fail_count += 1
            logging.error(f"  ↳ ❌ 重試失敗: {result}")
            results.append({
                'name': task_name,
                'progress': progress,
                'status': 'fail',
                'message': str(result)
            })
        
        sleep(2)  # 避免請求過於頻繁，增加延遲
    
    logging.info(f"✅ 帳號{account}重試完成: 成功 {success_count}, 失敗 {fail_count}")
    return success_count, fail_count, results


# 記錄批量任務結果並發送匯總
def record_batch_result(batch_id, status, name, message, update, context):
    global batch_results
    if not batch_id:
        return

    with batch_lock:
        if batch_id not in batch_results:
            return

        batch_results[batch_id]['processed'] += 1
        batch_results[batch_id]['results'].append({
            'name': name,
            'status': status,
            'message': message
        })
        
        # 檢查是否所有任務都已處理完畢
        if batch_results[batch_id]['processed'] == batch_results[batch_id]['total']:
            # 發送匯總通知
            results = batch_results[batch_id]['results']
            success_count = sum(1 for r in results if r['status'] == 'success')
            fail_count = sum(1 for r in results if r['status'] == 'fail')
            
            summary = f"📋 <b>下載任務匯總 (Batch Summary)</b>\n"
            summary += f"-------------------------\n"
            summary += f"✅ 成功: {success_count}\n"
            summary += f"❌ 失敗: {fail_count}\n"
            summary += f"-------------------------\n"
            
            for i, res in enumerate(results, 1):
                icon = "✅" if res['status'] == 'success' else "❌"
                summary += f"{i}. {icon} {res['name']}\n"
                if res['message']:
                     summary += f"   └ {res['message']}\n"

            # Check if context and update are valid (might be None for Web requests)
            if context and update and update.effective_chat:
                try:
                    context.bot.send_message(chat_id=update.effective_chat.id, text=summary, parse_mode='HTML')
                except Exception as e:
                    logging.error(f"發送匯總通知失敗: {e}")
            
            # 清理記錄
            del batch_results[batch_id]


# /pikpak命令主程序
def main(update: Update, context: CallbackContext, magnet, offline_path=None, batch_id=None, resume_task=None, target_account=None):
    # 磁链的简化表示，不保证兼容所有磁链，仅为显示信息时比较简介，不影响任何实际功能
    mag_url_simple = magnet
    if resume_task:
        mag_url_simple = f"恢復任務: {resume_task.get('name', 'Unknown')}"
    elif str(magnet).startswith("magnet:?"):
        mag_url_part = re.search(r'^(magnet:\?).*(xt=.+?)(&|$)', magnet)
        mag_url_simple = ''.join(mag_url_part.groups()[:-1])

    # Helper function to safely send messages
    def safe_send_message(text, parse_mode=None):
        try:
            if context and update and update.effective_chat:
                context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=parse_mode)
            else:
                # Fallback for startup recovery or internal calls
                if ADMIN_IDS:
                    updater.bot.send_message(chat_id=ADMIN_IDS[0], text=text, parse_mode=parse_mode)
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")

    try:  # 捕捉所有的请求超时异常
        for each_account in USER:
            # 如果是恢復模式，跳過非目標帳號
            if resume_task and each_account != target_account:
                continue

            # 离线下载并获取任务id和文件名
            mag_id, mag_name = None, None
            
            if resume_task:
                mag_id = resume_task['id']
                mag_name = resume_task['name']
                logging.info(f"正在恢復帳號 {each_account} 的任務: {mag_name}")
            else:
                for tries in range(3):
                    try:
                        mag_id, mag_name = magnet_upload(magnet, each_account, offline_path=offline_path)
                        if mag_id: # 成功獲取到ID
                            break
                    except requests.exceptions.ReadTimeout:
                        logging.warning(f"帳號{each_account}添加磁力鏈接超時，重試第{tries + 1}/3次...")
                        sleep(2)
                    except Exception as e:
                        logging.warning(f"帳號{each_account}添加磁力鏈接發生錯誤: {e}，重試第{tries + 1}/3次...")
                        sleep(2)

            if not mag_id:  # 如果添加离线失败，那就试试下一个账号
                if each_account == USER[-1]:  # 最后一个账号仍然无法离线下载
                    print_info = f'{mag_url_simple}所有帳號均離線下載失敗！可能是所有帳號免費離線次數用盡，或者檔案大小超過雲端硬碟剩餘容量！'
                    safe_send_message(print_info)
                    logging.warning(print_info)
                    record_batch_result(batch_id, 'fail', mag_url_simple, "所有帳號離線失敗", update, context)
                    return
                continue

            # 查询是否离线完成
            done = False  # 是否完成标志
            logging.info('5s後將檢查離線下載進度...')
            sleep(5)  # 等待5秒，一般是秒离线，可以保证大多数情况下直接就完成了离线下载
            offline_start = time()  # 离线开始时间
            not_found_count = 0
            while (not done) and (time() - offline_start < 60 * 60):  # 1小时超时
                try:
                    temp = get_offline_list(each_account)  # 获取离线列表
                    find = False  # 离线列表中找到了任务id的标志
                    for each_down in temp:
                        if each_down['id'] == mag_id:  # 匹配上任务id就是找到了
                            find = True
                            not_found_count = 0
                            
                            # 檢查是否已刪除 (點 2)
                            msg = each_down.get('message', '')
                            if "file deleted" in msg.lower() or "file_deleted" in msg.lower():
                                logging.info(f"帳號{each_account}離線任務 {mag_name} 檔案已在雲端刪除，跳過處理")
                                find = False # 視為未找到，這將導致 main 返回而不進行後續下載
                                break

                            if each_down['progress'] == 100 and msg == 'Saved':  # 查看完成了吗
                                done = True
                                file_id = each_down['file_id']
                                # 输出信息
                                print_info = f'帳號{each_account}離線下載磁力已完成：\n{mag_url_simple}\n檔案名稱：{mag_name}'
                                safe_send_message(print_info)
                                logging.info(print_info)
                            elif each_down['progress'] == 100:  # 可能存在错误但还是允许推送aria2下载了
                                done = True
                                file_id = each_down['file_id']
                                # 输出信息
                                print_info = f'帳號{each_account}離線下載磁力已完成:\n{mag_url_simple}\n但含有訊息：' \
                                             f'{msg.strip()}！\n檔案名稱：{mag_name}'
                                safe_send_message(print_info)
                                logging.warning(print_info)
                            else:
                                # 嘗試獲取文件名以便顯示更友好的日誌
                                current_file_name = each_down.get('file_name') or each_down.get('name') or mag_name or mag_url_simple
                                logging.info(
                                    f'帳號{each_account}離線下載 "{current_file_name}" 還未完成，進度{each_down["progress"]}%...'
                                )
                                sleep(10)
                            # 只要找到了就可以退出查找循环
                            break
                    # 非正常退出查询离线完成方式
                    if not find:  # 一轮下来没找到可能是删除或者添加失败等等异常
                        not_found_count += 1
                        if not_found_count >= 5:
                            print_info = f'帳號{each_account}離線下載{mag_url_simple}的任務被取消（或多次查詢未找到）！'
                            safe_send_message(print_info)
                            logging.warning(print_info)
                            break
                        else:
                            logging.warning(f"帳號{each_account}未找到任務{mag_id}，重試({not_found_count}/5)...")
                            sleep(5)
                            continue
                except Exception as e:
                    logging.warning(f"監控離線下載進度時發生錯誤 (將自動重試): {e}")
                    sleep(5)
                    continue

            # 查询账号是否完成离线
            if (find and done) or (not find and not done):  # 前者找到离线任务并且完成了，后者是要么手动取消了要么卡在进度0
                if not done:
                     # 離線失敗/取消
                     record_batch_result(batch_id, 'fail', mag_name if mag_name else mag_url_simple, "離線任務被取消或失敗", update, context)
                     return
                break
            elif find and not done:
                print_info = f'帳號{each_account}離線下載{mag_url_simple}的任務超時（1小時）！已取消該任務！'
                safe_send_message(print_info)
                logging.warning(print_info)
                record_batch_result(batch_id, 'fail', mag_name if mag_name else mag_url_simple, "離線下載超時", update, context)
                return
            else:  # 其他情况都换个号再试
                continue

        # 如果找到了任务并且任务已完成，则开始从网盘下载到本地
        if mag_id and find and done:  # 判断mag_id是否为空防止所有号次数用尽的情况
            gid = {}  # 记录每个下载任务的gid，{gid:[文件名,file_id,下载直链]}
            # 偶尔会出现aria2下载失败，报ssl i/o error错误，试试加上headers
            download_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'}

            down_name, down_url = get_download_url(file_id, each_account)
            # 获取到文件夹
            if down_url == "":
                logging.info(f"磁力{mag_url_simple}內容為資料夾:{down_name}，準備提取出每個檔案並下載")

                for name, url, down_file_id, path in get_folder_all_file(file_id, f"{down_name}/", each_account):
                    jsonreq = json.dumps({'jsonrpc': '2.0', 'id': 'qwer', 'method': 'aria2.addUri',
                                          'params': [f"token:{ARIA2_SECRET}", [url],
                                                     {"dir": ARIA2_DOWNLOAD_PATH + '/' + path, "out": f"{name}",
                                                      "header": download_headers}]})

                    push_flag = False  # 成功推送aria2下载标志
                    # 文件夹的推送下载是网络请求密集地之一，每个链接将尝试5次
                    for tries in range(5):
                        try:
                            response = requests.post(f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc', data=jsonreq,
                                                     timeout=5).json()
                            push_flag = True
                            break
                        except requests.exceptions.ReadTimeout:
                            logging.warning(f'{name}第{tries + 1}(/5)次推送下載超時，將重試！')
                            continue
                        except json.JSONDecodeError:
                            logging.warning(f'{name}第{tries + 1}(/5)次推送下載出錯，可能是frp故障，將重試！')
                            sleep(5)  # frp问题就休息一会
                            continue
                    if not push_flag:  # 5次都推送下载失败，让用户手动下载该文件，并且要检查网络！
                        print_info = f'{name}推送aria2下載失敗！該檔案直連如下，請手動下載：\n{url}'
                        safe_send_message(print_info)
                        logging.error(print_info)
                        continue  # 这个文件让用户手动下载，程序处理下一个文件

                    gid[response['result']] = [f'{name}', down_file_id, url]
                    # context.bot.send_message(chat_id=update.effective_chat.id, text=f'{name}推送aria2下载')  # 注释掉防止发送消息过多
                    logging.info(f'{path}{name}推送aria2下載')

                # 文件夹所有文件都推送完后再发送信息，避免消息过多
                safe_send_message(f'資料夾已推送aria2下載：\n{down_name}\n請耐心等待...')
                logging.info(f'{down_name}資料夾下所有檔案已推送aria2下載，請耐心等待...')

            # 否则是单个文件，只推送一次，不用太担心网络请求出错
            else:
                logging.info(f'{mag_url_simple}內容為單檔案，將直接推送aria2下載')

                jsonreq = json.dumps({'jsonrpc': '2.0', 'id': 'qwer', 'method': 'aria2.addUri',
                                      'params': [f"token:{ARIA2_SECRET}", [down_url],
                                                 {"dir": ARIA2_DOWNLOAD_PATH, "out": down_name,
                                                  "header": download_headers}]})
                
                push_flag = False
                for tries in range(5):
                    try:
                        response = requests.post(f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc', data=jsonreq,
                                                 timeout=5).json()
                        push_flag = True
                        break
                    except requests.exceptions.ReadTimeout:
                        logging.warning(f'{down_name}第{tries + 1}(/5)次推送aria2下載超時，將重試！')
                        continue
                    except json.JSONDecodeError:
                        logging.warning(f'{down_name}第{tries + 1}(/5)次推送aria2下載出錯，可能是frp故障，將重試！')
                        sleep(5)
                        continue
                    except Exception as e:
                        logging.warning(f'{down_name}第{tries + 1}(/5)次推送aria2下載發生未知錯誤: {e}，將重試！')
                        sleep(2)
                        continue

                if not push_flag:
                    print_info = f'{down_name}推送aria2下載失敗（多次重試無效）！該檔案直連如下，請手動下載：\n{down_url}'
                    safe_send_message(print_info)
                    logging.error(print_info)
                    # 這裡應該要標記失敗並返回，或者讓它進入失敗邏輯
                    record_batch_result(batch_id, 'fail', down_name, "推送Aria2失敗", update, context)
                    return 

                gid[response['result']] = [down_name, file_id, down_url]
                safe_send_message(f'檔案已推送aria2下載：\n{down_name}\n請耐心等待...')
                logging.info(f'{down_name}已推送aria2下載，請耐心等待...')

            logging.info(f'睡眠30s，之後將開始查詢{down_name}下載進度...')
            # pikpak单文件限速6MB/s
            sleep(30)
            # 查询每个gid是否完成
            download_done = False
            complete_file_id = []  # 记录aria2下载成功的文件id
            failed_gid = {}  # 记录下载失败的gid
            while not download_done:
                temp_gid = gid.copy()  # 下面的操作仅对temp_gid进行，别污染gid
                for each_gid in gid.keys():
                    # 这里是网络请求最密集的地方，一次查询失败跳过即可
                    try:
                        jsonreq = json.dumps({'jsonrpc': '2.0', 'id': 'qwer', 'method': 'aria2.tellStatus',
                                              'params': [f"token:{ARIA2_SECRET}", each_gid,
                                                         ["gid", "status", "errorMessage", "dir"]]})
                        response = requests.post(f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc', data=jsonreq,
                                                 timeout=5).json()
                    except requests.exceptions.ReadTimeout:  # 超时就查询下一个gid，跳过一个无所谓的
                        logging.warning(f'查詢GID{each_gid}時網路請求超時，將跳過此次查詢！')
                        continue
                    except json.JSONDecodeError:
                        logging.warning(f'查詢GID{each_gid}時返回結果錯誤，可能是frp故障，將跳過此次查詢！')
                        sleep(5)  # frp的问题就休息一会
                        continue

                    try:  # 检查任务状态
                        status = response['result']['status']
                        if status == 'complete':  # 完成了删除对应的gid并记录成功下载
                            temp_gid.pop(each_gid)  # 不再查询此gid
                            complete_file_id.append(gid[each_gid][1])  # 将它记为已完成gid
                        elif status == 'error':  # 如果aria2下载产生error
                            error_message = response["result"]["errorMessage"]  # 识别错误信息
                            # 如果是这两种错误信息，可尝试重新推送aria2下载来解决
                            if error_message in ['No URI available.', 'SSL/TLS handshake failure: SSL I/O error']:
                                # 再次推送aria2下载
                                retry_down_name, retry_the_url = get_download_url(gid[each_gid][1], each_account)
                                # 这只可能是文件，不会是文件夹
                                jsonreq = json.dumps({'jsonrpc': '2.0', 'id': 'qwer', 'method': 'aria2.addUri',
                                                      'params': [f"token:{ARIA2_SECRET}", [retry_the_url],
                                                                 {"dir": response["result"]["dir"],
                                                                  "out": retry_down_name,
                                                                  "header": download_headers}]})
                                # 当失败文件较多时，这里也是网络请求密集地
                                repush_flag = False
                                for tries in range(5):
                                    try:
                                        response = requests.post(f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc',
                                                                 data=jsonreq, timeout=5).json()
                                        repush_flag = True
                                        break
                                    except requests.exceptions.ReadTimeout:
                                        logging.warning(
                                            f'{retry_down_name}下載異常後重新推送第{tries + 1}(/5)次網路請求超時！將重試')
                                        continue
                                    except json.JSONDecodeError:
                                        logging.warning(
                                            f'{retry_down_name}下載異常後重新推送第{tries + 1}(/5)次返回結果錯誤，可能是frp故障！將重試！')
                                        sleep(5)  # frp的问题就休息一会
                                        continue
                                if not repush_flag:  # ?次重新推送失败，则认为此文件下载失败，让用户手动下载
                                    print_info = f'{retry_down_name}下載異常後重新推送失敗！該檔案直連如下，請手動下載：\n{retry_the_url}'
                                    safe_send_message(print_info)
                                    logging.error(print_info)
                                    failed_gid[each_gid] = temp_gid.pop(each_gid)  # 5次都不成功，别管这个任务了，放弃吧没救了
                                    continue  # 程序将查询下一个gid

                                # 重新记录gid
                                temp_gid[response['result']] = [retry_down_name, gid[each_gid][1], retry_the_url]
                                # 删除旧的gid
                                temp_gid.pop(each_gid)
                                # 消息提示
                                logging.warning(
                                    f'aria2下載{gid[each_gid][0]}出錯！錯誤訊息：{error_message}\t此檔案已重新推送aria2下載！')
                            # 其他错误信息暂未遇到，先跳过处理
                            else:
                                print_info = f'aria2下載{gid[each_gid][0]}出錯！錯誤訊息：{error_message}\t該檔案直連如下，' \
                                             f'請手動下載並反饋bug：\n{gid[each_gid][2]}'
                                safe_send_message(print_info)
                                logging.warning(print_info)
                                failed_gid[each_gid] = temp_gid.pop(each_gid)  # 认为该任务失败

                    except KeyError:  # 此时任务可能已被手动删除
                        safe_send_message(f'aria2下載{gid[each_gid][0]}任務被刪除！')
                        logging.warning(f'aria2下載{gid[each_gid][0]}任務被刪除！')
                        failed_gid[each_gid] = temp_gid.pop(each_gid)  # 认为该任务失败

                # 判断完所有下载任务情况
                gid = temp_gid
                if len(gid) == 0:
                    download_done = True
                    print_info = f'aria2下載已完成：\n{down_name}\n共{len(complete_file_id) + len(failed_gid)}個檔案，' \
                                 f'其中{len(complete_file_id)}個成功，{len(failed_gid)}個失敗'
                    
                    # Log cleanup start
                    logging.info(f"Aria2下載完成，準備清理PikPak檔案... (成功: {len(complete_file_id)}, 失敗: {len(failed_gid)})")
                    sleep(2) # 等待一小段時間確保狀態同步

                    # 输出下载失败的文件信息
                    if len(failed_gid):
                        print_info += '，下載失敗檔案為：\n'
                        for values in failed_gid.values():
                            print_info += values[0] + '\n'

                        # 存在失败文件则只释放成功文件的网盘空间
                        # 增加重試機制確保刪除成功
                        status_a = False
                        status_b = False
                        for _ in range(3):
                            if not status_a:
                                status_a = delete_files(complete_file_id, each_account)
                            if not status_b:
                                status_b = delete_trash(complete_file_id, each_account)
                            if status_a and status_b:
                                break
                            sleep(2)

                        if status_a:
                            logging.info(f'帳號{each_account}已刪除{down_name}中下載成功的雲端硬碟檔案')
                        if status_b:
                            logging.info(f'帳號{each_account}已刪除{down_name}中下載成功的垃圾桶檔案')
                        
                        if status_a and status_b:
                            print_info += f'帳號{each_account}中下載成功的雲端硬碟檔案已刪除\n'
                        elif each_account in AUTO_DELETE and AUTO_DELETE[each_account] == 'False':
                            print_info += f'帳號{each_account}未開啟自動刪除\n'
                        else:
                            print_info += f'帳號{each_account}中下載成功的雲端硬碟檔案刪除失敗，請手動刪除\n'

                        safe_send_message(print_info)
                        logging.info(print_info)

                        # /download命令仅打算临时解决问题，当/pikpak命令足够健壮后将弃用/download命令
                        print_info = f'對於下載失敗的檔案可使用指令：\n`/clean {each_account}`清空此帳號下所有檔案\n~~或者使用臨時指令：~~' \
                                     f'\n~~`/download {each_account}`重試下載此帳號下所有檔案~~'
                        safe_send_message(print_info, parse_mode='Markdown')
                        logging.info(print_info)
                        # 記錄批量失敗
                        record_batch_result(batch_id, 'fail', down_name, f"部分檔案下載失敗: {len(failed_gid)}個", update, context)
                    else:
                        # 没有失败文件，则直接删除该文件根目录
                        # 增加重試機制確保刪除成功
                        status_a = False
                        status_b = False
                        for _ in range(3):
                            if not status_a:
                                status_a = delete_files(file_id, each_account)
                            if not status_b:
                                status_b = delete_trash(file_id, each_account)
                            if status_a and status_b:
                                break
                            sleep(2)

                        if status_a:
                            logging.info(f'帳號{each_account}已刪除{down_name}雲端硬碟檔案')
                        if status_b:
                            logging.info(f'帳號{each_account}已刪除{down_name}垃圾桶檔案')
                        
                        if status_a and status_b:
                            print_info += f'\n帳號{each_account}中該檔案的雲端硬碟空間已釋放'
                        elif each_account in AUTO_DELETE and AUTO_DELETE[each_account] == 'False':
                            print_info += f'\n帳號{each_account}未開啟自動刪除'
                        else:
                            print_info += f'\n帳號{each_account}中該檔案的雲端硬碟空間釋放失敗，請手動刪除'
                        # 发送下载结果统计信息
                        safe_send_message(print_info)
                        logging.info(print_info)
                        
                        # 記錄批量成功
                        record_batch_result(batch_id, 'success', down_name, "", update, context)
                else:
                    logging.info(f'aria2下載{down_name}還未完成，睡眠20s後進行下一次查詢...')
                    sleep(20)

    except requests.exceptions.ReadTimeout:
        # 即使發生超時，也不要直接判定失敗，因為可能是查詢狀態時的短暫超時
        logging.warning(f'下載磁力{mag_url_simple}期間發生網路請求超時，但任務可能仍在進行中。')
        # 不發送失敗通知，讓它自然結束或由其他邏輯處理
    except Exception as e:
        logging.error(f"處理磁力{mag_url_simple}時發生未知錯誤: {e}")
        record_batch_result(batch_id, 'fail', mag_url_simple, f"發生未知錯誤: {str(e)}", update, context)


def pikpak(update: Update, context: CallbackContext):
    # 判断是文本消息还是命令消息
    if context.args is None:
        argv = update.message.text.split()
    else:
        argv = context.args  # 获取命令参数

    if len(argv) == 0:  # 如果仅为/pikpak命令，没有附带参数则返回帮助信息
        context.bot.send_message(chat_id=update.effective_chat.id, text='【用法】\n/p magnet1 [magnet2] [...]')
    else:
        print_info = '下載隊列添加離線磁力任務：\n'  # 将要输出的信息
        if os.path.isabs(argv[0]):
            temp_offline_path = argv[0]
            argv = argv[1:]
        else:
            temp_offline_path = None

        offline_path = None
        if temp_offline_path:
            offline_path = temp_offline_path
        elif str(PIKPAK_OFFLINE_PATH) not in ["None", "/My Pack"]:
            offline_path = PIKPAK_OFFLINE_PATH
        if offline_path:
            print_info += f'檢測到自定義下載路徑 {offline_path}，將離線到此路徑\n'
            logging.info(f'檢測到自定義下載路徑 {offline_path}，將離線到此路徑')

        # 初始化批量任務追蹤
        batch_id = str(uuid.uuid4())[:8]
        with batch_lock:
             batch_results[batch_id] = {
                 'total': len(argv),
                 'processed': 0,
                 'results': []
             }

        for each_magnet in argv:  # 逐个判断每个参数是否为磁力链接，并提取出
            # 一个磁链一个线程，此线程负责从离线到aria2下本地全过程
            thread_list.append(threading.Thread(target=main, args=[update, context, each_magnet, offline_path, batch_id]))
            thread_list[-1].start()

            # 显示信息为了简洁，仅提取磁链中xt参数部分
            mag_url_part = re.search(r'^(magnet:\?).*(xt=.+?)(&|$)', each_magnet)
            if mag_url_part:  # 正则匹配上，則输出信息
                print_info += ''.join(mag_url_part.groups()[:-1])
            else:  # 否则输出未识别信息
                print_info += each_magnet
            print_info += '\n\n'

        context.bot.send_message(chat_id=update.effective_chat.id, text=print_info.rstrip())
        logging.info(print_info.rstrip())


def check_download_thread_status():
    global thread_list
    thread_list = [i for i in thread_list if i.is_alive()]

    # 未完成返回True，完成返回False，类似running标志
    if len(thread_list):
        return True
    else:
        return False


def clean(update: Update, context: CallbackContext):
    argv = context.args  # 获取命令参数

    # 清空网盘应该阻塞住进程，防止一边下一边删
    if len(argv) == 0:  # 直接/clean则显示帮助
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text='【用法】\n' 
                                      '`/clean all`\t清空所有帳號雲端硬碟+離線任務記錄\n'
                                      '`/clean deep`\t深度清理（檔案+回收站+所有離線任務記錄）\n'
                                      '`/clean tasks`\t只清理離線任務記錄（不刪檔案）\n'
                                      '`/clean tasks error`\t只清理失敗的離線任務記錄\n'
                                      '/clean 帳號1 [帳號2] [...]\t清空指定帳號',
                                 parse_mode='Markdown')

    # 如果未完成
    elif check_download_thread_status():
        context.bot.send_message(chat_id=update.effective_chat.id, text='其他指令正在運行，為避免衝突，請稍後再試~')

    # 深度清理：檔案 + 回收站 + 離線任務記錄
    elif argv[0] in ['d', 'deep']:
        context.bot.send_message(chat_id=update.effective_chat.id, text='🔄 開始深度清理...')
        for temp_account in USER:
            login(temp_account)
            msg_parts = []
            
            # 1. 刪除所有檔案
            all_file_id = list(get_folder_all(temp_account))
            if len(all_file_id) > 0:
                delete_files(all_file_id, temp_account, mode='all')
                msg_parts.append(f"已刪除 {len(all_file_id)} 個檔案")
            
            # 2. 清空回收站
            if empty_trash(temp_account):
                msg_parts.append("回收站已清空")
            
            # 3. 刪除所有離線任務記錄
            success, fail = delete_offline_tasks(temp_account)
            if success > 0:
                msg_parts.append(f"已清理 {success} 個離線任務記錄")
            
            if msg_parts:
                result_msg = f'帳號{temp_account}深度清理完成:\n' + '\n'.join(f'  ✅ {p}' for p in msg_parts)
            else:
                result_msg = f'帳號{temp_account}無需清理'
            
            context.bot.send_message(chat_id=update.effective_chat.id, text=result_msg)
            logging.info(result_msg)

    # 只清理離線任務記錄
    elif argv[0] in ['t', 'tasks']:
        phase_filter = None
        if len(argv) >= 2 and argv[1] in ['e', 'error']:
            phase_filter = 'PHASE_TYPE_ERROR'
            context.bot.send_message(chat_id=update.effective_chat.id, text='🔄 正在清理失敗的離線任務記錄...')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text='🔄 正在清理所有離線任務記錄...')
        
        for temp_account in USER:
            login(temp_account)
            success, fail = delete_offline_tasks(temp_account, phase_filter=phase_filter)
            if success > 0 or fail > 0:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f'帳號{temp_account}離線任務記錄清理完成: ✅ {success} 個成功, ❌ {fail} 個失敗'
                )
            else:
                context.bot.send_message(chat_id=update.effective_chat.id, text=f'帳號{temp_account}沒有需要清理的離線任務記錄')

    elif argv[0] in ['a', 'all']:
        context.bot.send_message(chat_id=update.effective_chat.id, text='🔄 開始清空所有帳號...')
        for temp_account in USER:
            login(temp_account)
            msg_parts = []
            
            # 1. 刪除檔案
            all_file_id = list(get_folder_all(temp_account))
            if len(all_file_id) > 0:
                delete_files(all_file_id, temp_account, mode='all')
                delete_trash(all_file_id, temp_account, mode='all')
                msg_parts.append(f"已刪除 {len(all_file_id)} 個檔案")
            
            # 2. 清理已完成和失敗的離線任務記錄
            success, fail = delete_offline_tasks(temp_account, phase_filter='PHASE_TYPE_ERROR')
            if success > 0:
                msg_parts.append(f"已清理 {success} 個失敗的離線任務記錄")
            
            if msg_parts:
                result_msg = f'帳號{temp_account}清空完成:\n' + '\n'.join(f'  ✅ {p}' for p in msg_parts)
            else:
                result_msg = f'帳號{temp_account}雲端硬碟無需清空'
            
            context.bot.send_message(chat_id=update.effective_chat.id, text=result_msg)
            logging.info(result_msg)

    else:
        for each_account in argv:  # 输入参数是账户名称
            if each_account in USER:
                login(each_account)
                msg_parts = []
                
                # 1. 刪除檔案
                all_file_id = list(get_folder_all(each_account))
                if len(all_file_id) > 0:
                    delete_files(all_file_id, each_account, mode='all')
                    delete_trash(all_file_id, each_account, mode='all')
                    msg_parts.append(f"已刪除 {len(all_file_id)} 個檔案")
                
                # 2. 清理失敗的離線任務記錄
                success, fail = delete_offline_tasks(each_account, phase_filter='PHASE_TYPE_ERROR')
                if success > 0:
                    msg_parts.append(f"已清理 {success} 個失敗的離線任務記錄")
                
                if msg_parts:
                    result_msg = f'帳號{each_account}清空完成:\n' + '\n'.join(f'  ✅ {p}' for p in msg_parts)
                else:
                    result_msg = f'帳號{each_account}雲端硬碟無需清空'
                
                context.bot.send_message(chat_id=update.effective_chat.id, text=result_msg)
                logging.info(result_msg)

            else:
                context.bot.send_message(chat_id=update.effective_chat.id, text=f'帳號{each_account}不存在！')
                continue


# 打印账号和是否vip
def print_user_vip():
    print_info = '帳號      vip\n'
    for each_user in USER:
        flag = get_my_vip(each_user)
        if flag == 0:
            flag = '√'
        elif flag == 1:
            flag = '×'
        elif flag == 2:
            flag = '?'
        else:
            flag = '××'  # 登陆失败，检查账号密码
        print_info += f' `{each_user}`\[{flag}]\n'
    return print_info.rstrip()


# 仅打印账号
def print_user():
    print_info = "帳號：\n"
    for each_user in USER:
        print_info += f'`{each_user}`\n'
    return print_info.rstrip()


# 打印账号和密码
def print_user_pd():
    print_info = "帳號：\n"
    for each_user, each_password in zip(USER, PASSWORD):
        print_info += f'`{each_user}`\n`{each_password}`\n\n'
    return print_info.rstrip()


# 打印账号自动删除状态
def print_user_auto_delete():
    print_info = "帳號      自動清理\n"
    for key, value in AUTO_DELETE.items():
        print_info += f'`{key}`\[{value}]\n'
    return print_info.rstrip()


# 写config.py文件
def record_config():
    # 写入同目录下的config.py文件
    with open(os.path.abspath(os.path.dirname(__file__)) + '/config.py', 'w') as f:
        f.write(
            f'TOKEN = "{TOKEN}"\n'
            f'ADMIN_IDS = {ADMIN_IDS}\n'
            f'USER = {USER}\n'
            f'PASSWORD = {PASSWORD}\n'
            f'AUTO_DELETE = {AUTO_DELETE}\n'
            f'ARIA2_HTTPS = {ARIA2_HTTPS}\n'
            f'ARIA2_HOST = "{ARIA2_HOST}"\n'
            f'ARIA2_PORT = "{ARIA2_PORT}"\n'
            f'ARIA2_SECRET = "{ARIA2_SECRET}"\n'
            f'ARIA2_DOWNLOAD_PATH = "{ARIA2_DOWNLOAD_PATH}"\n'
            f'TG_API_URL = "{TG_API_URL}"\n'
            f'PIKPAK_OFFLINE_PATH = "{PIKPAK_OFFLINE_PATH}"\n')
    logging.info('已更新config.py文件')


# 判断是否为vip
def get_my_vip(account):
    try:
        login_headers = get_headers(account)

        me_url = f"{PIKPAK_API_URL}/drive/v1/privilege/vip"
        me_result = requests.get(url=me_url, headers=login_headers, timeout=5).json()
    except Exception:
        return 3

    if "error" in me_result:
        if me_result['error_code'] == 16:
            logging.info(f"帳號{account}登入過期，正在重新登入")
            login(account)
            login_headers = get_headers(account)
            me_result = requests.get(url=me_url, headers=login_headers, timeout=5).json()
        else:
            logging.error(f"獲取vip訊息失敗{me_result['error_description']}")
            return 3

    if me_result['data']['status'] == 'ok':
        return 0
    elif me_result['data']['status'] == 'invalid':
        return 1
    else:  # 暂未见过
        return 2


# 账号管理功能
def account_manage(update: Update, context: CallbackContext):
    # account l/list --> 账号名称 是否为 vip
    # account a/add 账号 密码 --> 添加到USER、PASSWORD开头，pikpak_headers开头加个元素None，保存到config.py
    # account d/delete 账号 --> 删除指定USER\PASSWORD\pikpak_headers
    argv = context.args
    # print(argv)

    if len(argv) == 0:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text='【用法】\n' 
                                      '羅列帳號：/account l/list \[pd]\[vip]\[status]\n' 
                                      '添加帳號：/account a/add 帳號 密碼\n' 
                                      '刪除帳號：/account d/delete 帳號1\n' 
                                      '註冊帳號：/account n/new\n' 
                                      '是否開啟清空雲端硬碟（預設開啟）：\n' 
                                      '/account on 帳號1 帳號2\n' 
                                      '/account off 帳號1 帳號2\n' 
                                      '【範例】\n' 
                                      '`/account l`\n' 
                                      '`/account l vip`\n' 
                                      '`/account l status`\n' 
                                      '`/account a` 123@qq.com 123\n' 
                                      '`/account d` 123@qq.com\n' 
                                      '`/account n`\n' 
                                      '`/account on` 123@qq.com\n' 
                                      '`/account off` 123@qq.com',
                                 parse_mode='Markdown')

    elif argv[0] in ['l', 'list']:
        if len(argv) == 2 and argv[1] == 'vip':
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_user_vip(), parse_mode='Markdown')
        elif len(argv) == 2 and argv[1] == 'status':
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_user_auto_delete(),
                                     parse_mode='Markdown')
        elif len(argv) == 2 and argv[1] == 'pd':
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_user_pd(), parse_mode='Markdown')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_user(), parse_mode='Markdown')

    elif argv[0] in ['a', 'add']:
        if len(argv) == 3:  # 三个参数才是正确形式
            USER.insert(0, argv[1])  # 插入账号
            PASSWORD.insert(0, argv[2])  # 插入密码
            pikpak_headers.insert(0, None)  # 设置pikpak_headers
            record_config()  # 记录进入config文件

            print_info = print_user()
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_info, parse_mode='Markdown')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text='參數個數錯誤，請檢查！')

    elif argv[0] in ['n', 'new']:
        if len(argv) == 1:  # 一个参数才是正确形式
            register = registerFuc()
            if register:
                USER.insert(0, register['account'])
                PASSWORD.insert(0, register['password'])
                pikpak_headers.insert(0, None)  # 设置pikpak_headers
                record_config()  # 记录进入config文件
                print_info = print_user()
                context.bot.send_message(chat_id=update.effective_chat.id, text=print_info, parse_mode='Markdown')
            else:
                context.bot.send_message(chat_id=update.effective_chat.id, text='註冊失敗，請重試！')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text='參數個數錯誤，請檢查！')

    elif argv[0] in ['d', 'delete']:
        if len(argv) > 1:
            for each_account in argv[1:]:
                try:
                    temp_account_index = USER.index(each_account)
                except ValueError:
                    context.bot.send_message(chat_id=update.effective_chat.id, text=f'帳號{each_account}不存在')
                    continue
                USER.pop(temp_account_index)
                PASSWORD.pop(temp_account_index)
                pikpak_headers.pop(temp_account_index)

                # 解决删除账号后，自动删除状态也要删除
                # 先判断是否存在，存在则删除
                if each_account in AUTO_DELETE:
                    AUTO_DELETE.pop(each_account)
                # 如果存在于AUTO_DELETE但是不存在于USER中，也要删除，这是历史遗留问题
                for key in list(AUTO_DELETE.keys()):
                    if key not in USER:
                        AUTO_DELETE.pop(key)

                record_config()

                print_info = print_user()
                context.bot.send_message(chat_id=update.effective_chat.id, text=print_info, parse_mode='Markdown')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text='參數個數錯誤，請檢查！')

    elif argv[0] in ['on', 'off']:
        if len(argv) > 1:
            for each_account in argv[1:]:
                try:
                    if each_account not in USER:
                        context.bot.send_message(chat_id=update.effective_chat.id, text=f'帳號{each_account}不存在')
                        continue
                    if argv[0] == 'on':
                        AUTO_DELETE[each_account] = 'True'
                    elif argv[0] == 'off':
                        AUTO_DELETE[each_account] = 'False'
                except ValueError:
                    context.bot.send_message(chat_id=update.effective_chat.id, text=f'帳號{each_account}不存在')
                    continue
            record_config()
            print_info = print_user_auto_delete()
            context.bot.send_message(chat_id=update.effective_chat.id, text=print_info, parse_mode='Markdown')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text='參數個數錯誤，請檢查！')
    else:
        context.bot.send_message(chat_id=update.effective_chat.id, text='不存在的指令語法！')


def path(update: Update, context: CallbackContext):
    """設置網盤離線下載路徑"""
    argv = context.args  # 獲取命令參數
    global PIKPAK_OFFLINE_PATH
    if len(argv) == 0:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text='【用法】\n' 
                                      '設置離線路徑：`/path 路徑參數`\n' 
                                      '查詢離線路徑：`/path info`\n' 
                                      '恢復預設路徑：`/path default`\n' 
                                      '【範例】\n' 
                                      '`/path /downloads`\n' 
                                      '路徑參數請使用絕對路徑，如`/downloads`',
                                 parse_mode='Markdown')
    elif argv[0] == 'info':
        if PIKPAK_OFFLINE_PATH == "None":
            context.bot.send_message(chat_id=update.effective_chat.id, text='當前離線下載路徑為預設路徑：`/My Pack`', parse_mode='Markdown')
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'當前離線下載路徑為：`{PIKPAK_OFFLINE_PATH}`', parse_mode='Markdown')
    elif argv[0] == 'default':
        PIKPAK_OFFLINE_PATH = "None"
        record_config()
        context.bot.send_message(chat_id=update.effective_chat.id, text='已恢復預設路徑：`/My Pack`', parse_mode='Markdown')
    else:
        # 判断路径是否为绝对路径
        if not os.path.isabs(argv[0]):
            context.bot.send_message(chat_id=update.effective_chat.id, text='路徑參數請使用絕對路徑或指令不存在！')
            return
        PIKPAK_OFFLINE_PATH = argv[0]
        record_config()
        context.bot.send_message(chat_id=update.effective_chat.id, text=f'已設置離線下載路徑：`{PIKPAK_OFFLINE_PATH}`', parse_mode='Markdown')


def retry(update: Update, context: CallbackContext):
    """重試卡住的離線下載任務"""
    argv = context.args
    
    # 預設進度閾值
    min_progress = 90
    
    # 解析參數
    if len(argv) >= 1:
        try:
            min_progress = int(argv[0])
            if min_progress < 0 or min_progress > 100:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text='進度閾值必須在 0-100 之間'
                )
                return
        except ValueError:
            if argv[0] not in ['list', 'l']:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text='【用法】\n'
                         '查看卡住的任務：`/retry list` 或 `/retry l`\n'
                         '重試卡住的任務：`/retry [進度閾值]`\n'
                         '【範例】\n'
                         '`/retry` - 重試進度 >= 90% 的任務\n'
                         '`/retry 99` - 重試進度 >= 99% 的任務\n'
                         '`/retry list` - 列出所有卡住的任務',
                    parse_mode='Markdown'
                )
                return
    
    # 處理 list 命令
    if len(argv) >= 1 and argv[0] in ['list', 'l']:
        list_min_progress = int(argv[1]) if len(argv) >= 2 else 90
        msg = f"📋 <b>卡住的任務列表</b> (進度 >= {list_min_progress}%)\n"
        msg += "─" * 25 + "\n"
        
        total_stuck = 0
        for account in USER:
            stuck = get_stuck_tasks(account, list_min_progress)
            if stuck:
                msg += f"\n<b>帳號: {account}</b>\n"
                for task in stuck:
                    msg += f"  • {task['name']} ({task['progress']}%)\n"
                total_stuck += len(stuck)
        
        if total_stuck == 0:
            msg += f"\n✅ 沒有找到卡住的任務"
        else:
            msg += f"\n─" + "─" * 24 + "\n"
            msg += f"共 {total_stuck} 個任務卡住"
        
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg,
            parse_mode='HTML'
        )
        return
    
    # 執行重試
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f'🔄 正在查找並重試進度 >= {min_progress}% 的卡住任務...'
    )
    
    total_success = 0
    total_fail = 0
    all_results = []
    
    for account in USER:
        success, fail, results = retry_stuck_tasks(account, min_progress, delete_cloud_files=True)
        total_success += success
        total_fail += fail
        if results:
            all_results.append({'account': account, 'results': results})
    
    # 發送結果
    if total_success + total_fail == 0:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f'✅ 沒有找到進度 >= {min_progress}% 的卡住任務'
        )
        return
    
    msg = f"📋 <b>重試結果</b>\n"
    msg += f"✅ 成功: {total_success}  ❌ 失敗: {total_fail}\n"
    
    # 只列出任務名稱（簡潔版）
    for item in all_results:
        for r in item['results']:
            icon = "✅" if r['status'] == 'success' else "❌"
            msg += f"{icon} {r['name']}\n"
    
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=msg,
        parse_mode='HTML'
    )


def alldown(update: Update, context: CallbackContext):
    """下载当前账号网盘内所有文件"""
    argv = context.args
    
    # 获取账号参数
    if len(argv) > 0:
        target_account = argv[0]
        if target_account not in USER:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f'帳號 {target_account} 不存在！\n可用帳號: {", ".join(USER)}'
            )
            return
    else:
        target_account = USER[0]
    
    # 检查是否有其他任务在运行
    if check_download_thread_status():
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text='其他指令正在運行，為避免衝突，請稍後再試~'
        )
        return
    
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f'🔄 開始掃描帳號 {target_account} 網盤內所有檔案...'
    )
    
    login(target_account)
    
    # 获取根目录下的所有文件
    all_files = []
    try:
        for name, url, file_id, path in get_folder_all_file('', '', target_account):
            all_files.append({
                'name': name,
                'url': url,
                'file_id': file_id,
                'path': path
            })
    except Exception as e:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f'❌ 掃描失敗: {e}'
        )
        logging.error(f'掃描帳號 {target_account} 網盤失敗: {e}')
        return
    
    if len(all_files) == 0:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f'✅ 帳號 {target_account} 網盤內沒有檔案'
        )
        return
    
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f'📊 找到 {len(all_files)} 個檔案，開始推送到 aria2 下載...'
    )
    
    # 推送到 aria2 下载
    download_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'
    }
    
    success_count = 0
    fail_count = 0
    failed_files = []
    
    for file_info in all_files:
        name = file_info['name']
        url = file_info['url']
        path = file_info['path']
        
        jsonreq = json.dumps({
            'jsonrpc': '2.0',
            'id': 'qwer',
            'method': 'aria2.addUri',
            'params': [
                f"token:{ARIA2_SECRET}",
                [url],
                {
                    "dir": ARIA2_DOWNLOAD_PATH + '/' + path,
                    "out": name,
                    "header": download_headers
                }
            ]
        })
        
        push_flag = False
        for tries in range(5):
            try:
                response = requests.post(
                    f'{SCHEMA}://{ARIA2_HOST}:{ARIA2_PORT}/jsonrpc',
                    data=jsonreq,
                    timeout=5
                ).json()
                push_flag = True
                break
            except requests.exceptions.ReadTimeout:
                logging.warning(f'{name}第{tries + 1}(/5)次推送aria2下載超時，將重試！')
                sleep(2)
                continue
            except Exception as e:
                logging.warning(f'{name}第{tries + 1}(/5)次推送aria2下載出錯: {e}，將重試！')
                sleep(2)
                continue
        
        if push_flag:
            success_count += 1
            logging.info(f'{path}{name}已推送aria2下載')
        else:
            fail_count += 1
            failed_files.append(f'{path}{name}')
            logging.error(f'{path}{name}推送aria2下載失敗（多次重試無效）')
    
    # 发送结果
    msg = f"📋 <b>下載結果</b>\n"
    msg += f"✅ 成功: {success_count}  ❌ 失敗: {fail_count}\n"
    
    if failed_files:
        msg += f"\n<b>失敗的檔案:</b>\n"
        for failed_file in failed_files[:10]:
            msg += f"  • {failed_file}\n"
        if len(failed_files) > 10:
            msg += f"  ... 還有 {len(failed_files) - 10} 個檔案\n"
    
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=msg,
        parse_mode='HTML'
    )


start_handler = CommandHandler(['start', 'help'], start)
pikpak_handler = CommandHandler('p', pikpak)
clean_handler = CommandHandler(['clean', 'clear'], clean)
account_handler = CommandHandler('account', account_manage)
path_handler = CommandHandler('path', path)
retry_handler = CommandHandler('retry', retry)
alldown_handler = CommandHandler('alldown', alldown)
magnet_handler = MessageHandler(Filters.regex('^magnet:\?xt=urn:btih:[0-9a-fA-F]{40,}.*$'), pikpak)

dispatcher.add_handler(AdminHandler())
dispatcher.add_handler(account_handler)
dispatcher.add_handler(start_handler)
dispatcher.add_handler(magnet_handler)
dispatcher.add_handler(pikpak_handler)
dispatcher.add_handler(clean_handler)
dispatcher.add_handler(path_handler)
dispatcher.add_handler(retry_handler)
dispatcher.add_handler(alldown_handler)

def startup_recovery():
    """Bot 啟動時檢查是否有未完成的任務並恢復監控"""
    logging.info("正在檢查是否有未完成的任務需要恢復...")
    try:
        for account in USER:
            # 獲取該帳號的所有離線任務
            tasks = get_offline_list(account)
            resumed_count = 0
            for task in tasks:
                # 篩選條件：狀態是正在下載 (RUNNING) 或 完成但未推送 (COMPLETE)
                # 注意：PikPak API 的 phase 可能是 PHASE_TYPE_RUNNING 或 PHASE_TYPE_COMPLETE
                phase = task.get('phase')
                progress = int(task.get('progress', 0))
                message = task.get('message', '')
                
                # 忽略已刪除的檔案 (點 2)
                if "file deleted" in message.lower() or "file_deleted" in message.lower():
                    continue
                
                # 篩選條件：狀態是正在下載 (RUNNING) 或 完成但未推送 (COMPLETE)
                if phase == 'PHASE_TYPE_RUNNING' or (phase == 'PHASE_TYPE_COMPLETE' and progress == 100):
                    task_info = {
                        'id': task.get('id'),
                        'name': task.get('name') or task.get('file_name')
                    }
                    # 啟動恢復線程
                    thread_list.append(threading.Thread(target=main, args=[None, None, None, None, None, task_info, account]))
                    thread_list[-1].start()
                    resumed_count += 1
                    sleep(1) # 避免過快啟動
            
            if resumed_count > 0:
                logging.info(f"已從帳號 {account} 恢復 {resumed_count} 個任務")
    except Exception as e:
        logging.error(f"啟動恢復任務失敗: {e}")

# 啟動恢復線程
recovery_thread = threading.Thread(target=startup_recovery)
recovery_thread.daemon = True
recovery_thread.start()

# 啟動 Web UI 線程
flask_thread = threading.Thread(target=run_flask)
flask_thread.daemon = True
flask_thread.start()

port = int(globals().get('WEB_PORT', 5000))
logging.info(f"Web UI 已啟動，請訪問 http://localhost:{port}")

updater.start_polling()
updater.idle()
