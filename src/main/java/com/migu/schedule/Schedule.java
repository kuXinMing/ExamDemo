package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/*
*类名和方法不能修改
 */
public class Schedule {
    Map<Integer,List<TaskInfo>> taskInfoMap = new HashMap<Integer,List<TaskInfo>>();
    //挂起任务
    List<TaskInfo> hangTaskInfos = new ArrayList<TaskInfo>();
    Queue<TaskInfo> hangTaskInfoQueue = new ArrayBlockingQueue<TaskInfo>(100);
    public int init() {
        taskInfoMap.clear();
        hangTaskInfos.clear();
        if(taskInfoMap.size()==0 || taskInfoMap == null || taskInfoMap.isEmpty()){
            return ReturnCodeKeys.E001;
        }
        return ReturnCodeKeys.E000;
    }


    public int registerNode(int nodeId) {
        if(nodeId<=0){
            //服务节点编号非法
            return ReturnCodeKeys.E004;
        }
        Integer nodeName = getKey(nodeId);
        if(taskInfoMap.containsKey(nodeName)){
            //服务节点已注册
            return ReturnCodeKeys.E005;
        }
        List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
        taskInfoMap.put(nodeName,taskInfos);
        //注册成功
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if(nodeId<=0){
            //服务节点编号非法
            return ReturnCodeKeys.E004;
        }
        Integer nodeName = getKey(nodeId);

        if(taskInfoMap.containsKey(nodeName)){
            List<TaskInfo> hangTackInfo = taskInfoMap.remove(nodeName);
            if(null!=hangTackInfo && hangTackInfo.size()!=0){
                //有任务，则挂起任务
                hangTaskInfoQueue.addAll(hangTackInfo);
            }
            return ReturnCodeKeys.E006;
        }
        else{
            //服务节点不存在
            return ReturnCodeKeys.E007;
        }

    }


    public int addTask(int taskId, int consumption) {
        if(taskId<=0){
            //服务节点编号非法
            return ReturnCodeKeys.E009;
        }
        if(!taskInfoMap.isEmpty()){
            for (List<TaskInfo> taskInfos : taskInfoMap.values()) {
                for (TaskInfo taskInfo :
                        taskInfos) {
                    if(taskId==taskInfo.getTaskId()){
                        //任务已添加
                        return ReturnCodeKeys.E010;
                    }
                }
            }
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setConsumption(consumption);
            Integer nodeKey = leastTaskNode();
            taskInfo.setNodeId(nodeKey);
            taskInfoMap.get(nodeKey).add(taskInfo);

            return ReturnCodeKeys.E008;
        }
        return 0;
    }


    public int deleteTask(int taskId) {
        if(taskId<=0){
            //服务节点编号非法
            return ReturnCodeKeys.E009;
        }
        for (List<TaskInfo> taskInfos : taskInfoMap.values()) {
            for (TaskInfo taskInfo :
                    taskInfos) {
                if(taskId==taskInfo.getTaskId()){
                    taskInfos.remove(taskInfo);
                    return ReturnCodeKeys.E011;
                }
            }
        }
        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        //如果挂起队列中有任务存在
        if(hangTaskInfoQueue.size()!=0){

        }
        //如果没有挂起的任务
        else {

        }
        // TODO 方法未实现
        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        // TODO 方法未实现
        return ReturnCodeKeys.E000;
    }

    public int getKey( int nodeId){
        return nodeId;
    }

    /**
     * 任务调度策略实现
     * @return
     */
    public boolean scheduleTaskStrategy(int threshold){
        //获取节点数
        int nodeSize = taskInfoMap.size();
        //获取所有任务资源消耗值(key是任务Id,value是资源消耗)
        Map<Integer,Integer> taskConsumValMap =getTaskConsumptionMap();
        Collection<Integer> consumptionSet = taskConsumValMap.values();
        //对所有占用资源进行排序 小到大
        List<Integer> consumptionList = new ArrayList<>();
        consumptionList.addAll(consumptionSet);
//        List<List<Integer>> consumptionSets = averageAssign(consumptionList,threshold);
        return false;
    }

    public Map<Integer,Integer> getTaskConsumptionMap(){
        //获取所有任务资源消耗值(key是资源消耗，value是任务Id)
        Map<Integer,Integer> taskConsumValMap = new HashMap<>();
        for (Map.Entry<Integer,List<TaskInfo>> entry : taskInfoMap.entrySet()) {
            for (TaskInfo taskInfo:
                    entry.getValue()) {
                taskConsumValMap.put(taskInfo.getConsumption(),taskInfo.getTaskId());
            }
        }
        return taskConsumValMap;
    }
    /**
     * 取资源消耗最少的节点
     * @return
     */
    public Integer leastTaskNode(){
        //节点Map，(key是节点Id，value是资源消耗)
        Map<Integer,Integer> nodeSizeMap = new HashMap<>();
        for (Map.Entry<Integer,List<TaskInfo>> entry : taskInfoMap.entrySet()) {
            nodeSizeMap.put(entry.getKey(),taskConsumptionum(entry.getValue()));
        }
        //获取所有Value
        Collection<Integer> consumptionums = nodeSizeMap.values();
        //优化优化优化优化优化
        return getMapValue(nodeSizeMap,leastSet(consumptionums));
    }
    //策略选取最小的资源消耗率节点存储任务
    public Integer leastSet(Collection<Integer> keySet){
        Integer inter = Integer.MAX_VALUE;
        for (Integer integer:
                keySet) {
            if(integer<inter){
                inter=integer;
            }

        }
        return inter;
    }
    //计算节点下面所有的任务资源消耗总和
    public Integer taskConsumptionum(List<TaskInfo> taskInfos){
        Integer inter = 0;
        for (TaskInfo taskInfo:
                taskInfos) {
            inter += taskInfo.getConsumption();

        }
        return inter;
    }

    public Integer getMapValue(Map<Integer,Integer> map,Integer Value){
        Integer key=0;
        for (Map.Entry<Integer,Integer> entry : map.entrySet()) {
            if(Value==entry.getValue()){
                key=entry.getKey();
            }
        }
        return key;
    }

}
