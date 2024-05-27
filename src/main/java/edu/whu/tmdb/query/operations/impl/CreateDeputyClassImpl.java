package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private final MemConnect memConnect;

    public CreateDeputyClassImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        // 1.获取代理类名、代理类型、select元组
        String deputyClassName = stmt.getDeputyClass().toString();  // 代理类名
        if (memConnect.classExist(deputyClassName)) {
            throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
        }
        int deputyType = getDeputyType(stmt);   // 代理类型
        Select selectStmt = stmt.getSelect();
        SelectResult selectResult = getSelectResult(selectStmt);

        // 2.执行代理类创建
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType,stmt.getSelect().toString());
        // System.out.println(deputyId);
        createDeputyTableItem(selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }

    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClassName) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType,"");
        createDeputyTableItem(selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }



    /**
     * get the origin class classid
     * @param oriId
     * @param alias
     * @return
     */
    private int getOriAttrId(int oriId, String alias) {
        
        for (int i = 0; i < MemConnect.getClassTable().classTableList.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClassTable().classTableList.get(i);
            if(classTableItem.classid==oriId && classTableItem.attrname.equals(alias)){
                return classTableItem.attrid;
                
            }
        }
        return -1;
    }

    /**
     * 创建代理类的实现，包含代理类classTableItem的创建和switchingTableItem的创建
     * @param deputyClassName 代理类名称
     * @param selectResult 代理类包含的元组列表
     * @param deputyRule 代理规则
     * @return 新建代理类ID
     */
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule,String DetailDeputyRule) throws TMDBException {
        // 遍历selectResult
        for (ClassTableItem item : MemConnect.getClassTableList()) {
            if (item.classname.equals(deputyClassName)) {
                throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
            }
        }

        MemConnect.getClassTable().maxid++;
        int classid = MemConnect.getClassTable().maxid;//代理类的id
        int count=selectResult.getAttrid().length;//代理类的长度

        // 1.新建classTableItem  使用MemConnect.getClassTableList().add()
        //第一步，创建代理类，代理类的classtype设置为de
        //同时，在switchingtable中插入源属性到代理属性的映射
        for (int i = 0; i < selectResult.getAttrid().length; i++) {
            int Attrid =selectResult.getAttrid()[i];
            MemConnect.getClassTableList().add(
                    new ClassTableItem(deputyClassName,classid,count,Attrid,selectResult.getAttrname()[i],selectResult.getType()[i],
                            "de",selectResult.getAlias()[i]
                            ));
            // 2.新建switchingTableItem
            // 使用MemConnect.getSwitchingTableList().add()
            String className=selectResult.getClassName()[i];
            int oriId=memConnect.getClassId(className);
            int oriAttrId=getOriAttrId(oriId,selectResult.getAttrname()[i]);

            MemConnect.getSwitchingTableList().add(new SwitchingTableItem(oriId,oriAttrId,selectResult.getAttrname()[i],classid,i,selectResult.getAlias()[i],String.valueOf(deputyRule)+DetailDeputyRule));
        }
        return classid;
    }

    /**
     * 新建deputyTableItem
     * @param classNames 源类类名列表
     * @param deputyType 代理规则
     * @param deputyId 代理类id
     */
    public void createDeputyTableItem(String[] classNames, int deputyType, int deputyId) throws TMDBException {
        // 使用MemConnect.getDeputyTableList().add()
        HashSet<String> collectclassNames = Arrays.stream(classNames).collect(Collectors.toCollection(HashSet::new));
        for (String name :collectclassNames) {
            int oriId=memConnect.getClassId(name);
            MemConnect.getDeputyTableList().add(
                    new DeputyTableItem(oriId,deputyId,new String[]{deputyType+""})
            );
        }
    }

    /**
     * 插入元组，并新建BiPointerTableItem
     * @param selectResult 插入的元组列表
     * @param deputyId 新建代理类id
     */
    private void createBiPointerTableItem(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        // 使用MemConnect.getBiPointerTableList().add()插入BiPointerTable
        // 1. Insert each item in the selectResult
        TupleList tpl=selectResult.getTpl();
        InsertImpl insert=new InsertImpl();
        List<String> columns= Arrays.asList(selectResult.getAttrname());

        for (int i = 0; i < tpl.tuplelist.size(); i++) {
            Tuple tuple=selectResult.getTpl().tuplelist.get(i);
            // 使用insert.execute()插入对象
            int deputyTupleId = insert.execute(deputyId, columns, new Tuple(tuple.tuple));
            // 可调用getOriginClass(selectResult);
            HashSet<Integer> origin = getOriginClass(selectResult);
            for (int origin_index :origin) {
                int classId=memConnect.getClassId(selectResult.getClassName()[origin_index]);
                int oriTupleId=tuple.tupleIds[origin_index];
                // System.out.println(deputyId);
                // System.out.println(deputyTupleId);
                MemConnect.getBiPointerTableList().add(
                        new BiPointerTableItem(classId,oriTupleId,deputyId,deputyTupleId)
                );
            }
        }
    }

    /**
     * 给定创建代理类语句，返回代理规则
     * @param stmt 创建代理类语句
     * @return 代理规则
     */
    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)) {
            case "selectdeputy":    return 0;
            case "joindeputy":      return 1;
            case "uniondeputy":     return 2;
            case "groupbydeputy":   return 3;
        }
        return -1;
    }

    /**
     * 给定查询语句，返回select查询执行结果（创建deputyclass后面的select语句中的selectResult）
     * @param selectStmt select查询语句
     * @return 查询执行结果（包含所有满足条件元组）
     */
    private SelectResult getSelectResult(Select selectStmt) throws TMDBException, IOException {
        SelectImpl selectExecutor = new SelectImpl();
        return selectExecutor.select(selectStmt);
    }

    private HashSet<Integer> getOriginClass(SelectResult selectResult) {
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res = new HashSet<>();
        for (String s : collect1) {
            res.add(collect.indexOf(s));
        }
        return res;
    }
}
