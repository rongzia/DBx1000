//
// Created by rrzhang on 2020/4/27.
//

#include "myhelper.h"

namespace dbx1000 {
    int MyHelper::RCToInt(RC rc) {
        if(RCOK == rc)   { return 0;}
        if(Commit == rc) { return 1;}
        if(Abort == rc)  { return 2;}
        if(WAIT == rc)   { return 3;}
        if(ERROR == rc)  { return 4;}
        if(FINISH == rc) { return 5;}
    }

    RC MyHelper::IntToRC(int i) {
        if(0 == i) { return RCOK; }
        if(1 == i) { return Commit; }
        if(2 == i) { return Abort; }
        if(3 == i) { return WAIT; }
        if(4 == i) { return ERROR; }
        if(5 == i) { return FINISH; }
    }

    int MyHelper::AccessToInt(access_t type) {
        if(RD == type)   { return 0;}
        if(WR == type)   { return 1;}
        if(XP == type)   { return 2;}
        if(SCAN == type) { return 3;}
    }

    access_t MyHelper::IntToAccess(int i) {
        if(0 == i) { return RD; }
        if(1 == i) { return WR; }
        if(2 == i) { return XP; }
        if(3 == i) { return SCAN; }
    }

    int MyHelper::TsTypeToInt(TsType ts_type) {
        if(R_REQ == ts_type)  { return 0; }
        if(W_REQ == ts_type)  { return 1; }
        if(P_REQ == ts_type)  { return 2; }
        if(XP_REQ == ts_type) { return 3; }
    }

    TsType MyHelper::IntToTsType(int i) {
        if(0 == i) { return R_REQ; }
        if(1 == i) { return W_REQ; }
        if(2 == i) { return P_REQ; }
        if(3 == i) { return XP_REQ; }
    }

    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> MyHelper::GetSysClock() {
        return std::chrono::system_clock::now();
    }
}