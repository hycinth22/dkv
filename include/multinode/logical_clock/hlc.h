#include "dkv_utils.hpp"
#include <algorithm>

struct HLCTimepoint {
    int64_t time;
    int64_t counter;
    bool operator==(const HLCTimepoint& rhs) const {
        return time==rhs.time && counter==rhs.counter;
    }
    bool operator!=(const HLCTimepoint& rhs) const {
        return !(*this == rhs);
    }
    bool operator<(const HLCTimepoint& rhs) const {
        if(time<rhs.time) return true;
        return counter<rhs.counter;
    }
    bool operator>(const HLCTimepoint& rhs) const {
        if(time>rhs.time) return true;
        return counter>rhs.counter;
    }
    bool operator<=(const HLCTimepoint& rhs) const {
        return !(*this > rhs);
    }
    bool operator>=(const HLCTimepoint& rhs) const {
        return !(*this < rhs);
    }
};

struct HybridLogicalClock {
private:
    int64_t time;
    int64_t counter;

    void onLocalEvent() {
        int64_t old_time = time;
        time = std::max(time, dkv::getLocalTime()); // no back
        if (time > old_time) {
            counter = 0;
        } else {
            counter++;
        }
    }

    void onReceivedEvent(const HLCTimepoint& remote_hlc) {
        int64_t old_time = time;
        time = std::max({time, dkv::getLocalTime(), remote_hlc.time}); // no back
        if (time == old_time && remote_hlc.time == old_time) {
            counter = std::max(counter, remote_hlc.counter) + 1;
        } else if (time == old_time) { // local newest and keeping
            counter++;
        } else if (time == remote_hlc.time) { // remote newest and keeping
            counter = remote_hlc.counter + 1;
        } else {
            counter = 0; // new phsical time
        }
    }
public:
    HybridLogicalClock() : time(0), counter(0)
    {}

    HLCTimepoint getTime() {
        onLocalEvent();
        return HLCTimepoint{.time=time, .counter=counter};
    }

    int64_t getPhysicalTime() {
        onLocalEvent();
        return time;
    }

    void update(const HLCTimepoint& remote_hlc) {
        onReceivedEvent(remote_hlc);
    }

    // void update(int64_t remote_time) {
    //     HLCTimepoint remote_hlc{.time=remote_time, .counter=0};
    //     onReceivedEvent(remote_hlc);
    // }

};

