package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Drop;
import edu.whu.tmdb.query.operations.utils.MemConnect;

public class DropImpl implements Drop {

    private MemConnect memConnect;

    public DropImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String tableName = drop.getName().getName();
        int classId = memConnect.getClassId(tableName);
        drop(classId);
        return true;
    }

    public void drop(int classId) {
        // TODO-task4
        ArrayList<Integer> deputyClassIdList = new ArrayList<>();   // 存储该类对应所有代理类id

        dropClassTable(classId);                            // 1.删除ClassTableItem
        dropDeputyClassTable(classId, deputyClassIdList);   // 2.获取代理类id并在表中删除
        dropBiPointerTable(classId);                        // 3.删除 源类/对象<->代理类/对象 的双向关系表
        dropSwitchingTable(classId);                        // 4.删除switchingTable
        dropObjectTable(classId);                           // 5.删除已创建的源类对象

        // 6.递归删除代理类相关
        // TODO-task4
    }

    /**
     * 给定要删除的class id，删除系统表类表(class table)中的表项
     * @param classId 要删除的表对应的id
     */
    private void dropClassTable(int classId) {
        // TODO-task4
        ArrayList<ClassTableItem> tempC=new ArrayList<>();
        for (int i = 0; i <  MemConnect.getClassTableList().size(); i++) {
            ClassTableItem classTableItem =MemConnect.getClassTableList().get(i);
            if(classTableItem.classid==classId){
                tempC.add(classTableItem);
            }
        }
        for (ClassTableItem temp :
                tempC) {
            MemConnect.getClassTableList().remove(temp);
        }
    }

    /**
     * 删除系统表中的deputy table，并获取class id对应源类的代理类id
     * @param classId 源类id
     * @param deputyClassIdList 作为返回值，源类对应的代理类id列表
     */
    private void dropDeputyClassTable(int classId, ArrayList<Integer> deputyClassIdList) {
        ArrayList<DeputyTableItem> tempD=new ArrayList<>();
        ArrayList<Integer> toDrop=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyTableList().size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyTableList().get(i);
            if(deputyTableItem.originid==classId){
                toDrop.add(deputyTableItem.deputyid);
                tempD.add(deputyTableItem);
            }
        }
        for(DeputyTableItem temp: tempD){
            MemConnect.getDeputyTableList().remove(temp);
        }
    }

    /**
     * 删除系统表中的BiPointerTable
     * @param classId 源类id
     */
    private void dropBiPointerTable(int classId) {
        ArrayList<BiPointerTableItem> tempB=new ArrayList<>();
        for (int i = 0; i < MemConnect.getBiPointerTableList().size(); i++) {
            BiPointerTableItem biPointerTableItem = MemConnect.getBiPointerTableList().get(i);
            if(biPointerTableItem.objectid==classId || biPointerTableItem.deputyobjectid==classId){
                tempB.add(biPointerTableItem);
            }
        }
        for(BiPointerTableItem temp:tempB){
            MemConnect.getBiPointerTableList().remove(temp);
        }
    }

    /**
     * 删除系统表中的SwitchingTable
     * @param classId 源类id
     */
    private void dropSwitchingTable(int classId) {
        // TODO-task4
        ArrayList<SwitchingTableItem> tempS=new ArrayList<>();
        for (int i = 0; i < MemConnect.getSwitchingTableList().size(); i++) {
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingTableList().get(i);
            if(switchingTableItem.oriId==classId || switchingTableItem.deputyId==classId){
                tempS.add(switchingTableItem);
            }
        }
        for(SwitchingTableItem temp:tempS){
            MemConnect.getSwitchingTableList().remove(temp);
        }
    }

    /**
     * 删除源类具有的所有对象的列表
     * @param classId 源类id
     */
    private void dropObjectTable(int classId) {
        // TODO-task4
        // 使用MemConnect.getObjectTableList().remove();
        ArrayList<ObjectTableItem> tempT=new ArrayList<>();
        ArrayList<Integer> toDrop=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyTableList().size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyTableList().get(i);
            if(deputyTableItem.originid==classId){
                toDrop.add(deputyTableItem.deputyid);
            }
        }
        for (int i = 0; i < MemConnect.getObjectTableList().size(); i++) {
            ObjectTableItem objectTableItem = MemConnect.getObjectTableList().get(i);
            if(objectTableItem.classid==classId ){
                memConnect.DeleteTuple(objectTableItem.tupleid);
                tempT.add(objectTableItem);
            }
        }
        for(ObjectTableItem temp:tempT){
            MemConnect.getObjectTableList().remove(temp);
        }
        if(toDrop.isEmpty()){
            return;
        }
        for (int i = 0; i < toDrop.size(); i++) {
            drop(toDrop.get(i));
        }
    }

}
