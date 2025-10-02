#!/bin/bash
#SBATCH --job-name=bfs_project
#SBATCH --partition=Centaurus
#SBATCH --output=bfs_%j.out
#SBATCH --error=bfs_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:20:00

# Load required modules
module load gcc/11.2.0
module load curl

# Go to the correct project directory
cd ~/blocking-queue/blocking-queue/sequential || { echo "Directory not found"; exit 1; }

# Compile the client (with RapidJSON include)
g++ -std=c++17 -O2 -pthread client.cpp -I ~/rapidjson/include -o client -lcurl

# Test compilation
if [[ ! -f client ]]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

# Sequential BFS
echo "Running sequential BFS..."
./client "Tom Hanks" 4 seq > sequential.log 2>&1

# Parallel BFS with 8 threads
echo "Running parallel BFS with 8 threads..."
./client "Tom Hanks" 4 par 8 > parallel.log 2>&1

echo "BFS runs completed."
