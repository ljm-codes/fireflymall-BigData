package com.fm.Tools.Agg.UserBehavior;

import com.fm.ODS.DWM.userIpAndUsername;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class UserIPCountAgg implements AggregateFunction<Tuple2<String, String>, userIpAndUsername, userIpAndUsername> {

    @Override
    public userIpAndUsername createAccumulator() {
        return new userIpAndUsername("", new CopyOnWriteArrayList<>(), 0, 0);
    }

    @Override
    public userIpAndUsername add(Tuple2<String, String> stringStringTuple2, userIpAndUsername userIpAndUsername) {
        userIpAndUsername.setUserIP(stringStringTuple2.f0);

        List<String> usernameList = userIpAndUsername.getUsernameList();

        if (!usernameList.contains(stringStringTuple2.f1)) {
            usernameList.add(stringStringTuple2.f1);
            userIpAndUsername.setCount(usernameList.size());
        }

        return userIpAndUsername;
    }

    @Override
    public userIpAndUsername getResult(userIpAndUsername userIpAndUsername) {
        userIpAndUsername.setCount(userIpAndUsername.getUsernameList().size());
        return userIpAndUsername;
    }

    @Override
    public userIpAndUsername merge(userIpAndUsername acc1, userIpAndUsername acc2) {
        if (acc1 == null) {
            return acc2;
        }
        if(acc2 == null) {
            return acc1;
        }
        String targetIP = acc1.getUserIP() != null && !acc1.getUserIP().isEmpty() ? acc1.getUserIP() : acc2.getUserIP();
        acc1.setUserIP(targetIP);

        List<String> acc1List = acc1.getUsernameList();
        List<String> acc2List = acc2.getUsernameList();

        for(String username: acc2List) {
            if(username != null && !username.trim().isEmpty() && !acc1List.contains(username)){
                acc1List.add(username);
            }
        }

        acc1.setCount(acc1List.size());

        return acc2;
    }

}
