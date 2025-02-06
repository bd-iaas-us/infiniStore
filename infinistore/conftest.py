import subprocess
import sys


def install_packages():
    packages = ["pytest", "pytest-cov", "pytest-benchmark"]
    for package in packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            print(f"Installing missing package: {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])


install_packages()
