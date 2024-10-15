import subprocess
from setuptools import setup, find_packages, Extension
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
        if self.inplace:
            # developer mode
            print("developer mode: building shared library")
            subprocess.check_call(["make", "clean"], cwd="src")
            subprocess.check_call(["make"], cwd="src")
            super().run()
        else:
            # package mode, return. build.sh script will build the shared library
            return


commit_count = get_git_commit_count()
cpp_extension = Extension(
    name="infinistore._infinistore",
    sources=[],
)

setup(
    name="infinistore",
    version=f"0.1.{commit_count}",
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
    entry_points={
        "console_scripts": [
            "infinistore=infinistore.server:main",
        ],
    },
    ext_modules=[cpp_extension],
    zip_safe=False,
)
