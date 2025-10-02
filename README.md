# blocking-queue


**Project:** BFS traversal using a web API with blocking queue (parallel and sequential)  

---

## Files
- `client.cpp` — C++ code for BFS traversal  
- `Makefile` — compile client.cpp  
- `run_bfs.sbatch` — SLURM script for Centaurus  

---

## Compile
bash
cd ~/nlocking-queue/blocking-queue/sequential
make clean && make

## Run 
sbatch run_bfs.sbatch

## Expected Output:
Start node

Mode (seq or par)

Max depth

Number of visited nodes

Elapsed time

List of visited nodes


