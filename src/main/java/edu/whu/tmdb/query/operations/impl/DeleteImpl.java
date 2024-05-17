package edu.whu.tmdb.query.operations.impl;


import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

public class DeleteImpl implements Delete {

    public DeleteImpl() {}

    @Override
    public void delete(Statement statement) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public void execute(net.sf.jsqlparser.statement.delete.Delete deleteStmt) throws JSQLParserException, TMDBException, IOException {
        // 1.获取符合where条件的所有元组
        Table table = deleteStmt.getTable();        // 获取需要删除的表名
        Expression where = deleteStmt.getWhere();   // 获取delete中的where表达式
        String sql = "select * from " + table;;
        if (where != null) {
            sql += " where " + String.valueOf(where) + ";";
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse);

        // 2.执行delete
        delete(selectResult.getTpl());
    }

    public void delete(TupleList tupleList) throws TMDBException {
        // TODO-task8
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        ArrayList<Integer> delete=new ArrayList<>();
        // 1.删除源类tuple和object table
        // 使用MemConnect.getObjectTableList().remove();   // 删除对象表

//        // 创建源类元组id列表、类id列表
//        ArrayList<Integer> sourceTupleIdList = new ArrayList<Integer>();
//        ArrayList<Integer> sourceClassIdList = new ArrayList<Integer>();
//        for (int i = 0; i < tupleList.tuplelist.size(); i++)  {
//            int sourceTupleId = tupleList.tuplelist.get(i).getTupleId();
//            int sourceClassId = tupleList.tuplelist.get(i).classId;
//            sourceTupleIdList.add(sourceTupleId);
//            sourceClassIdList.add(sourceClassId);
//        }

        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            // 获取源类元组ID
            int sourceTupleId = tupleList.tuplelist.get(i).getTupleId();
            int sourceClassId = tupleList.tuplelist.get(i).classId;
            // 调用 memConnect.DeleteTuple 方法删除每个元组的ID
            memConnect.DeleteTuple(sourceTupleId);
            // 删除源类的对象表项
            ObjectTableItem objectTableItem =new ObjectTableItem(sourceClassId,sourceTupleId);
            // 删除源类的对象表
            MemConnect.getObjectTableList().remove(objectTableItem);
            delete.add(sourceTupleId);
        }

        ArrayList<Integer> deputyTupleIdList = new ArrayList<>();

        // 2.删除源类biPointerTable
        // 使用MemConnect.getBiPointerTableList().remove();
        for (int i = 0; i < MemConnect.getBiPointerTable().biPointerTableList.size(); i++) {
            BiPointerTableItem biPointerTableItem = MemConnect.getBiPointerTable().biPointerTableList.get(i);
            if(delete.contains(biPointerTableItem.objectid)){
                deputyTupleIdList.add(biPointerTableItem.deputyobjectid);
                MemConnect.getBiPointerTable().biPointerTableList.remove(biPointerTableItem);
            }
        }
//        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
//            int sourceClassId = tupleList.tuplelist.get(i).classId;
//            ArrayList<Integer> deputyTupleIdList = memConnect.getDeputyIdList(sourceClassId);

        // 3.根据biPointerTable递归删除代理类相关表
        if (deputyTupleIdList.isEmpty()) {return; }
        // 创建代理类元组列表
        TupleList deputyTupleList = new TupleList();
        for (Integer deputyTupleId : deputyTupleIdList) {
            Tuple deputyTuple = memConnect.GetTuple(deputyTupleId);
            deputyTupleList.tuplelist.add(deputyTuple);
        }
        // 递归删除代理类相关表
        delete(deputyTupleList);
    }
}
