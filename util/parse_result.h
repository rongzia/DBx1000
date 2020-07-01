//
// Created by rrzhang on 2020/7/1.
//

#ifndef DBX1000_PARESE_RESULT_H
#define DBX1000_PARESE_RESULT_H

void AppendRunTime(uint64_t run_time);
void AppendLatency(uint64_t latency);
void AppendThroughtput(uint64_t throughtput);
void AppendRemoteLockTime(uint64_t remoteLockTime);
void ParseRunTime();
void ParseLatency();
void ParseThroughtput();
void ParseRemoteLockTime();


#endif //DBX1000_PARESE_RESULT_H
