#include "dkv_raft_persist.h"
#include "dkv_logger.hpp"
#include <fstream>
#include <sstream>

namespace dkv {

// RAFT文件持久化构造函数
RaftFilePersister::RaftFilePersister(const std::string& dir) : dir_(dir) {
    // 初始化文件路径
    stateFilePath_ = dir_ + "/raft_state.txt";
    logFilePath_ = dir_ + "/raft_log.txt";
    snapshotFilePath_ = dir_ + "/raft_snapshot.bin";
}

// 保存状态
void RaftFilePersister::SaveState(int term, int votedFor) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ofstream file(stateFilePath_);
    if (file.is_open()) {
        file << term << " " << votedFor << std::endl;
        file.close();
    }
}

// 保存日志
void RaftFilePersister::SaveLog(const std::vector<RaftLogEntry>& log) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ofstream file(logFilePath_);
    if (file.is_open()) {
        for (const auto& entry : log) {
            file << entry.index << " " << entry.term << " " << entry.command.size() << " ";
            file.write(entry.command.data(), entry.command.size());
            file << std::endl;
        }
        file.close();
    }
}

// 保存快照
void RaftFilePersister::SaveSnapshot(const std::vector<char>& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ofstream file(snapshotFilePath_, std::ios::binary);
    if (file.is_open()) {
        file.write(snapshot.data(), snapshot.size());
        file.close();
    }
}

// 读取任期
int RaftFilePersister::ReadTerm() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ifstream file(stateFilePath_);
    if (file.is_open()) {
        int term, votedFor;
        if (file >> term >> votedFor) {
            file.close();
            return term;
        }
        file.close();
    }
    return 0;
}

// 读取投票给谁
int RaftFilePersister::ReadVotedFor() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ifstream file(stateFilePath_);
    if (file.is_open()) {
        int term, votedFor;
        if (file >> term >> votedFor) {
            file.close();
            return votedFor;
        }
        file.close();
    }
    return -1;
}

// 读取日志
std::vector<RaftLogEntry> RaftFilePersister::ReadLog() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<RaftLogEntry> log;
    std::ifstream file(logFilePath_);
    
    if (file.is_open()) {
        std::string line;
        while (getline(file, line)) {
            std::istringstream iss(line);
            int index, term, size;
            if (iss >> index >> term >> size) {
                // 跳过空格
                iss.ignore(1);
                
                // 读取命令数据
                std::vector<char> command(size);
                if (iss.read(command.data(), size)) {
                    RaftLogEntry entry;
                    entry.index = index;
                    entry.term = term;
                    entry.command = std::move(command);
                    log.push_back(entry);
                }
            }
        }
        file.close();
    }
    
    return log;
}

// 读取快照
std::vector<char> RaftFilePersister::ReadSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<char> snapshot;
    std::ifstream file(snapshotFilePath_, std::ios::binary | std::ios::ate);
    
    if (file.is_open()) {
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        snapshot.resize(size);
        if (file.read(snapshot.data(), size)) {
            file.close();
        }
    }
    
    return snapshot;
}

} // namespace dkv
