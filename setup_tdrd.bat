@echo off
setlocal

set CONDA_EXE=C:\ProgramData\anaconda3\Scripts\conda.exe

echo ============================================================
echo  TDRD Environment Setup
echo ============================================================

echo [1/5] Creating conda environment 'tdrd' (Python 3.10)...
call "%CONDA_EXE%" env create -f environment.yml
if errorlevel 1 (
    echo Environment already exists. Updating instead...
    call "%CONDA_EXE%" env update -f environment.yml --prune
)

echo [2/5] Installing PyTorch with CUDA 11.8...
call "%CONDA_EXE%" run -n tdrd pip install torch==2.2.0 --index-url https://download.pytorch.org/whl/cu118

echo [3/5] Verifying core imports...
call "%CONDA_EXE%" run -n tdrd python -c "import rasterio, geopandas, osmnx, httpx, cv2; print('  Core imports OK')"

echo [4/5] Verifying satellite imports...
call "%CONDA_EXE%" run -n tdrd python -c "import sentinelsat, pystac_client; print('  Satellite imports OK')"

echo [5/5] Verifying deep learning imports...
call "%CONDA_EXE%" run -n tdrd python -c "import torch; print(f'  PyTorch {torch.__version__}, CUDA={torch.cuda.is_available()}')"

echo.
echo ============================================================
echo  Setup complete. Activate with: conda activate tdrd
echo ============================================================
endlocal
