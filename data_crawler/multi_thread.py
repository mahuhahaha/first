import concurrent.futures
import subprocess

def run_script(script_name):
    # 使用 subprocess 运行脚本
    subprocess.run(["python", script_name])

if __name__ == "__main__":
    script_name = "crawler_main.py"  # 要运行的脚本文件
    num_instances = 10  # 同时运行的实例数量

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_instances) as executor:
        futures = [executor.submit(run_script, script_name) for _ in range(num_instances)]

    print("所有实例运行完成！")