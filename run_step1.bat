@echo off
cd /d "C:\Users\hp\OneDrive\Desktop\Dataset pipeline"
"C:\Users\hp\.conda\envs\firstenv\python.exe" -u main.py run-step1 >> "C:\Users\hp\OneDrive\Desktop\Dataset pipeline\logs\step1_run_final.log" 2>> "C:\Users\hp\OneDrive\Desktop\Dataset pipeline\logs\step1_run_final_err.log"
