import subprocess
import shutil
from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext


def get_git_commit_count():
    try:
        commit_count = (
            subprocess.check_output(["git", "rev-list", "--count", "HEAD"])
            .decode("utf-8")
            .strip()
        )
        return commit_count
    except subprocess.CalledProcessError:
        return "0"


# invoke the make command to build the shared library
class CustomBuildExt(build_ext):
    def run(self):
        # Run the make command in the src directory
        subprocess.check_call(["make"], cwd="src")
        # get result of 'python3-config --extension-suffix'
        suffix = (
            subprocess.check_output(["python3-config", "--extension-suffix"])
            .decode("utf-8")
            .strip()
        )
        # Ensure the .so file is copied to the correct place
        shutil.copy(f"src/_infinistore{suffix}", "infinistore")

        super().run()


commit_count = get_git_commit_count()

setup(
    name="infinistore",
    version=f"0.1+.{commit_count}",
    packages=find_packages(),
    cmdclass={"build_ext": CustomBuildExt},
    package_data={
        "infinistore": ["*.so"],
    },
    install_requires=["torch", "uvloop", "fastapi", "pybind11", "uvicorn"],
    description="A kvcache memory pool",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/bd-iaas-us/infiniStore",
)
