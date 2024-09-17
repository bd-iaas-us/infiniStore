import os
import subprocess, shutil
from setuptools import setup, find_packages, Extension
from setuptools.command.build_ext import build_ext

# invoke the make command to build the shared library
class CustomBuildExt(build_ext):
    def run(self):
        # Run the make command in the src directory
        subprocess.check_call(['make'], cwd='src')
        # get result of 'python3-config --extension-suffix'
        suffix = subprocess.check_output(['python3-config', '--extension-suffix']).decode('utf-8').strip()
        # Ensure the .so file is copied to the correct place
        shutil.copy(f'src/_infinity{suffix}', 'infinity')
        
        super().run()

setup(
    name="infinity",
    version="0.1",
    packages=find_packages(),
    cmdclass={'build_ext': CustomBuildExt},
    package_data={
        'infinity': ['*.so'],
    },
    install_requires=[
        'torch',
        'uvloop',
    ],
    description="A cluster kvcache memory store",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/bd-iaas-us/infinity",
)