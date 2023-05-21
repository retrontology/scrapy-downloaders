import requests
import os
import json
import socket
import multiprocessing
import progressbar
import time
import sys


def main():
    port = 42069
    ip = '192.168.1.1'
    pool = multiprocessing.get_context("spawn").Pool(4)
    processes = []
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((ip, port))
    serversocket.listen(5)
    end = False
    while not end:
        clientsocket, address = serversocket.accept()
        msg = clientsocket.recv(1024).decode()
        if msg == 'END':
            end = True
            clientsocket.send('OK'.encode())
            continue
        item = json.loads(msg)
        #print(item)
        os.makedirs(item['directory'], exist_ok=True)
        processes.append(pool.apply_async(download, args = (item['url'], os.path.join(item['directory'], item['name']))))
        clientsocket.send('OK'.encode())
    serversocket.close()
    pool.close()
    progress = progressbar.bar.ProgressBar(max_value=len(processes)).start()
    completed = 0
    while completed < len(processes):
        completed = 0
        for p in processes:
            if p.ready(): completed += 1
        progress.update(completed)
        time.sleep(1)
    progress.finish()
    pool.join()


def download(url, path, rename=False, retry=3):
    if os.path.exists(path) and not rename:
        print(path + " already exists! Skipping!")
    else:
        if os.path.exists(path) and rename:
            filename, ext = os.path.basename(path).rsplit('.', maxsplit=1)
            newPath = path
            count = 1
            while os.path.exists(newPath):
                newPath = os.path.join(os.path.dirname(path), filename + "[" + count + "]" + "." + ext)
                count = count + 1
            path = newPath
        print("Downloading: " + url + " to " + path)
        attempts = 0
        while attempts < retry:
            try:
                r = requests.get(url, allow_redirects=True, timeout=5, stream=True)
                if r.status_code == 200:
                    size = int(r.headers.get('Content-Length'))
                    with open(path+".part", 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024):
                            f.write(chunk)
                    os.rename(path+".part", path)
                    print("Downloaded: " + path)
                    break
                else: attempts += 1
            except:
                attempts += 1
                print(sys.exc_info()[0])
            if attempts == retry - 1:
                print("Could not download: " + url)


if __name__ == '__main__':
    main()
