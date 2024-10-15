PYTHON_VERSIONS=(
  "/opt/python/cp310-cp310/bin/python3.10"
  "/opt/python/cp311-cp311/bin/python3.11"
  "/opt/python/cp312-cp312/bin/python3.12"
)
rm -rf build/ dist/ wheelhouse/
for PYTHON in "${PYTHON_VERSIONS[@]}"; do
    make -C src clean
    make -C src manylinux PYTHON=${PYTHON} -j8
    if [ $? -ne 0 ]; then
        exit 1
    fi
    unset LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=/usr/local/lib:$($PYTHON -c "import sysconfig; print(sysconfig.get_paths()['purelib'])")/nvidia/cuda_runtime/lib/:/usr/local/lib64
    #ldd check, auditwheel will also check LD_LIBRARY_PATH
    ldd src/*.so
    ${PYTHON} setup.py bdist_wheel
    #runtime will install ibverbs, so exclude it
    WHEEL_FILE=$(ls dist/*.whl)
    echo "WHEEL_FILE: ${WHEEL_FILE}"
    auditwheel repair dist/* --exclude libibverbs.so.1
    rm -rf dist/
done
