#include "multinode/logical_clock/hlc.h"
#include "dkv_utils.hpp"
#include <gtest/gtest.h>

class LogicalClockTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code if needed
    }

    void TearDown() override {
        // Cleanup code if needed
    }
};

TEST_F(LogicalClockTest, TestHLCInitialization) {
    HybridLogicalClock hlc;
    auto time = hlc.getPhysicalTime();
    EXPECT_TRUE(time > 0);
}

TEST_F(LogicalClockTest, TestHLCTick) {
    HybridLogicalClock hlc;
    auto time1 = hlc.getTime();
    auto time2 = hlc.getTime();
    EXPECT_TRUE(time2 > time1);
}

TEST_F(LogicalClockTest, TestHLCMaxTime) {
    HybridLogicalClock hlc;
    auto currentTime = hlc.getTime();
    auto remoteTime = currentTime;
    remoteTime.time += 1000; // Remote time is 1 second ahead
    hlc.update(remoteTime);
    auto time = hlc.getTime();
    EXPECT_TRUE(time >= remoteTime);
}

TEST_F(LogicalClockTest, TestHLCUpdateWithOlderTime) {
    HybridLogicalClock hlc;
    auto currentTime = hlc.getTime();
    auto remoteTime = currentTime;
    remoteTime.time -= 1000; // Remote time is 1 second behind
    hlc.update(remoteTime);
    auto time = hlc.getTime();
    EXPECT_TRUE(time > currentTime);
}

TEST_F(LogicalClockTest, TestHLCConcurrency) {
    HybridLogicalClock hlc;
    std::vector<HLCTimepoint> times;
    const int numTicks = 100;

    for (int i = 0; i < numTicks; i++) {
        times.push_back(hlc.getTime());
    }

    // Check that all times are strictly increasing
    for (size_t i = 1; i < times.size(); i++) {
        EXPECT_TRUE(times[i] > times[i-1]);
    }
}

TEST_F(LogicalClockTest, TestHLCMultipleUpdates) {
    HybridLogicalClock hlc;
    
    // First update with a future time
    auto time1 = hlc.getTime();
    time1.time += 500;
    hlc.update(time1);
    auto current1 = hlc.getTime();
    EXPECT_TRUE(current1 >= time1);
    
    // Second update with an even later time
    auto time2 = time1;
    time2.time += 500;
    hlc.update(time2);
    auto current2 = hlc.getTime();
    EXPECT_TRUE(current2 >= time2);
    EXPECT_TRUE(current2 > current1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
