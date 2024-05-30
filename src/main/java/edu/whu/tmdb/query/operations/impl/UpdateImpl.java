package edu.whu.tmdb.query.operations.impl;

import edu.whu.tmdb.query.operations.Exception.ErrorList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.Update;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

import edu.whu.tmdb.query.operations.impl.InsertImpl;
import edu.whu.tmdb.query.operations.impl.DeleteImpl;

public class UpdateImpl implements Update {

    private final MemConnect memConnect;
    private InsertImpl insertImpl;
    private DeleteImpl deleteImpl;

    public UpdateImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
        insertImpl = new InsertImpl();
        deleteImpl = new DeleteImpl();
    }

    @Override
    public void update(Statement stmt) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.update.Update) stmt);
    }

    public void execute(net.sf.jsqlparser.statement.update.Update updateStmt)
            throws JSQLParserException, TMDBException, IOException {
        // 1.update语句(类名/属性名)存在性检测
        String updateTableName = updateStmt.getTable().getName();
        if (!memConnect.classExist(updateTableName)) {
            throw new TMDBException(ErrorList.CLASS_NAME_DOES_NOT_EXIST, updateTableName);
        }
        ArrayList<UpdateSet> updateSetStmts = updateStmt.getUpdateSets(); // update语句中set字段列表
        for (UpdateSet updateSetStmt : updateSetStmts) {
            String columnName = updateSetStmt.getColumns().get(0).getColumnName();
            if (!memConnect.columnExist(updateTableName, columnName)) {
                throw new TMDBException(ErrorList.COLUMN_NAME_DOES_NOT_EXIST, columnName);
            }
        }

        // 2.获取符合where条件的所有元组
        String sql = "select * from " + updateTableName + " where " + updateStmt.getWhere().toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
                .parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse); // 注：selectResult均为临时副本，不是源数据

        // 3.执行update操作
        int[] indexs = new int[updateSetStmts.size()]; // update中set语句修改的属性->类表中属性的映射关系
        Object[] updateValue = new Object[updateSetStmts.size()];
        setMapping(selectResult.getAttrname(), selectResult.getAlias(), updateSetStmts, indexs, updateValue);
        int classId = memConnect.getClassId(updateTableName);
        update(selectResult.getTpl(), indexs, updateValue, classId);
    }

    /**
     * update的具体执行过程
     * @param tupleList   经筛选得到的tuple list副本（只包含tuple属性）
     * @param indexs      update中set语句修改的属性->类表中属性的映射关系
     * @param updateValue set语句中的第i个对应于源类中第j个属性修改后的值
     * @param classId     修改表的id
     * @throws IOException 
     */
    public void update(TupleList tupleList, int[] indexs, Object[] updateValue, int classId) throws TMDBException, IOException {
        // 1.更新源类tuple
        ArrayList<Integer> updateIdList = new ArrayList<>();
        TupleList newTupleList = new TupleList();
        for (Tuple tuple : tupleList.tuplelist) {
            for (int i = 0; i < indexs.length; i++) {
                tuple.tuple[indexs[i]] = updateValue[i];
            }
            newTupleList.addTuple(tuple);
            memConnect.UpateTuple(tuple, tuple.getTupleId());
            updateIdList.add(tuple.getTupleId());
        }

        // 2.根据biPointerTable找到对应的deputyTuple
        ArrayList<Integer> deputyTupleIdList = new ArrayList<>();
        TupleList deputyTupleList = new TupleList();    // 所有代理类的元组
        for (BiPointerTableItem biPointerTableItem : MemConnect.getBiPointerTableList()) {
            if (updateIdList.contains(biPointerTableItem.objectid)) {
                deputyTupleIdList.add(biPointerTableItem.deputyobjectid);
                Tuple tuple = memConnect.GetTuple(biPointerTableItem.deputyobjectid);
                deputyTupleList.addTuple(tuple);
            }
        }
        if (deputyTupleIdList.isEmpty()) { return; }

        // 3.获取deputyTupleId->...的哈希映射列表
        List<Integer> collect = Arrays.stream(indexs).boxed().collect(Collectors.toList());
        HashMap<Integer, ArrayList<Integer>> deputyId2AttrId = new HashMap<>();         // 满足where条件的deputyId -> deputyAttrIdList(其实也是index)
        HashMap<Integer, ArrayList<Object>> deputyId2UpdateValue = new HashMap<>();     // 满足where条件的deputyId -> 更新后的属性值列表(其实也是updateValue)
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if (switchingTableItem.oriId == classId && collect.contains(switchingTableItem.oriAttrid)) {
                if (!deputyId2AttrId.containsKey(switchingTableItem.deputyId)) {
                    deputyId2AttrId.put(switchingTableItem.deputyId, new ArrayList<>());
                    deputyId2UpdateValue.put(switchingTableItem.deputyId, new ArrayList<>());
                }
                deputyId2AttrId.get(switchingTableItem.deputyId).add(switchingTableItem.deputyAttrId);
                int tempIndex = collect.indexOf(switchingTableItem.oriAttrid);
                deputyId2UpdateValue.get(switchingTableItem.deputyId).add(updateValue[tempIndex]);
            }
        }

        // 4.递归修改所有代理类
        ArrayList<Integer> DeputyIdList = memConnect.getDeputyIdList(classId);
        String[][] DeputyTypeList = memConnect.getDeputyTypeList(classId);
        SelectImpl select=new SelectImpl();
        for (int i = 0; i < DeputyIdList.size(); i++) {
            int deputyId = DeputyIdList.get(i);
            String[] deputyRules = DeputyTypeList[i];
            TupleList updateTupleList = new TupleList();
            for (Tuple tuple : deputyTupleList.tuplelist) { // 找到该代理类的所有相关元组
                if (tuple.classId == deputyId) {
                    updateTupleList.addTuple(tuple);    
                }
            }
            for (String deputyRule : deputyRules) {
                if (deputyRule.equals("0")) { // Select Deputy
                    int[] nextIndexs = deputyId2AttrId.get(deputyId).stream().mapToInt(Integer -> Integer).toArray();
                    Object[] nextUpdate = deputyId2UpdateValue.get(deputyId).toArray();
                    update(updateTupleList, nextIndexs, nextUpdate, deputyId);
                } 
                else if(deputyRule.equals("1")){ // Join Deputy
                    for (Tuple tuple : updateTupleList.tuplelist) {
                        deleteImpl.delete(deputyId, tuple.tupleId);
                    }
                    String deputyDetailRule = memConnect.getDetailDeputyRule(deputyId);    // 获取join的详细规则
                    List<Integer> anotherClassId = memConnect.getAnotherOriginID(deputyId, classId);    // join的结果的其它源类id
                    SelectResult selectResult = insertImpl.getDeputyJoinSelectResult(classId, newTupleList, anotherClassId, select, deputyDetailRule);    // 获取join的结果
                    createBiPointerTableItem(selectResult, deputyId);
                }
            }
        }
    }

    private void createBiPointerTableItem(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        // 使用MemConnect.getBiPointerTableList().add()插入BiPointerTable
        // 1. Insert each item in the selectResult
        TupleList tpl=selectResult.getTpl();
        InsertImpl insert=new InsertImpl();
        List<String> columns= Arrays.asList(selectResult.getAttrname());

        for (int i = 0; i < tpl.tuplelist.size(); i++) {
            Tuple tuple=selectResult.getTpl().tuplelist.get(i);
            // 使用insert.execute()插入对象
            try {
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
            } catch (TMDBException e) {
                ;
            }
        }
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

    public List<Tuple> getDeputyJoinTupleList(int thisClassID, TupleList tuplelist, List<Integer> anotherClassId, SelectImpl select,String DeputyDetailRule) throws TMDBException {
        List<Tuple> deputyInsertTupleList = new ArrayList<>(); //Result

        //获取另外一个类的所有Tuple->SelectResult
        TupleList anothertuple = new TupleList();
        List<ObjectTableItem> objs= MemConnect.getObjectTableList();
        for (ObjectTableItem obj : objs) {
            if (anotherClassId.contains(obj.classid)) {
                anothertuple.addTuple(memConnect.GetTuple(obj.tupleid));
            }
        }
        List<ClassTableItem> classTableItems = memConnect.getClassTableList(); // Assuming this method returns a list of all class table entries
        SelectResult  right = getSelectResultInformation(anotherClassId, classTableItems, anothertuple);

        //构建本类的SelectResult
        SelectResult  left =  getSelectResultInformation(thisClassID, classTableItems, tuplelist);

        SelectImpl selectImpl = new SelectImpl();
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(DeputyDetailRule.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            SelectBody selectBody = ((net.sf.jsqlparser.statement.select.Select)stmt).getSelectBody();
            PlainSelect plainSelect = (PlainSelect) selectBody;
            ArrayList<ClassTableItem> leftClassTableItemList = memConnect.copyClassTableList(left.getClassName()[0]);
            ArrayList<ClassTableItem> rightClassTableItemList = memConnect.copyClassTableList(right.getClassName()[0]);
            leftClassTableItemList.addAll(rightClassTableItemList);
            if(!(plainSelect.getJoins() == null)){
                for (Join join:plainSelect.getJoins()) {
                    TupleList tupleList = selectImpl.join(left,right,join);
                    deputyInsertTupleList = tupleList.tuplelist;
                    left=select.getSelectResult(leftClassTableItemList, tupleList);
                }
            }
            if (plainSelect.getWhere() != null){
                Where where = new Where();
                left = where.where(plainSelect, left);
            }
            left = select.projection(plainSelect, left);
        }
        catch (JSQLParserException e) {
            System.out.println("syntax error");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        deputyInsertTupleList = left.getTpl().tuplelist;
        return deputyInsertTupleList;
    }

    private SelectResult getSelectResultInformation(List<Integer> thisClassID, List<ClassTableItem> classTableItems, TupleList thisTupleList) {
        List<String> thisClassName = new ArrayList<>();     // 字段所属的类名
        List<String> thisAttrname = new ArrayList<>();      // 字段名
        List<String> thisAlias = new ArrayList<>();         // 字段的别名，在进行select时会用到
        List<Integer> thisAttrid = new ArrayList<>();       // 显示时使用
        List<String> thisType = new ArrayList<>();          // 字段数据类型(char, int)
        for (ClassTableItem item : classTableItems) {
            if (thisClassID.contains(item.classid)) {
                thisClassName.add(item.classname);
                thisAttrname.add(item.attrname);
                thisAlias.add(item.alias);
                thisAttrid.add(item.attrid);
                thisType.add(item.attrtype);
            }
        }
        SelectResult thisResult = new SelectResult(thisTupleList,
                thisClassName.toArray(new String[0]),
                thisAttrname.toArray(new String[0]),
                thisAlias.toArray(new String[0]),
                thisAttrid.stream().mapToInt(i->i).toArray(),
                thisType.toArray(new String[0]));
        return thisResult;
    }
    private SelectResult getSelectResultInformation(int thisClassID, List<ClassTableItem> classTableItems, TupleList thisTupleList) {
        List<String> thisClassName = new ArrayList<>();     // 字段所属的类名
        List<String> thisAttrname = new ArrayList<>();      // 字段名
        List<String> thisAlias = new ArrayList<>();         // 字段的别名，在进行select时会用到
        List<Integer> thisAttrid = new ArrayList<>();       // 显示时使用
        List<String> thisType = new ArrayList<>();          // 字段数据类型(char, int)
        for (ClassTableItem item : classTableItems) {
            if (item.classid == thisClassID) {
                thisClassName.add(item.classname);
                thisAttrname.add(item.attrname);
                thisAlias.add(item.alias);
                thisAttrid.add(item.attrid);
                thisType.add(item.attrtype);
            }
        }
        SelectResult thisResult = new SelectResult(thisTupleList,
                thisClassName.toArray(new String[0]),
                thisAttrname.toArray(new String[0]),
                thisAlias.toArray(new String[0]),
                thisAttrid.stream().mapToInt(i->i).toArray(),
                thisType.toArray(new String[0]));
        return thisResult;
    }


    /**
     * 给定attrNames和updateSetStmts，对indexs和updateValue进行赋值
     * 
     * @param attrNames      满足更新条件元组的属性名列表
     * @param updateSetStmts update语句set字段列表
     * @param indexs         赋值：set字段属性->元组属性的位置对应关系
     * @param updateValue    赋值：set字段赋值列表
     */
    private void setMapping(String[] attrNames, String[] aliasNames, ArrayList<UpdateSet> updateSetStmts, int[] indexs,
            Object[] updateValue) {
        for (int i = 0; i < updateSetStmts.size(); i++) {
            UpdateSet updateSet = updateSetStmts.get(i);
            for (int j = 0; j < attrNames.length; j++) {
                if (!updateSet.getColumns().get(0).getColumnName().equals(attrNames[j])
                 && !updateSet.getColumns().get(0).getColumnName().equals(aliasNames[j])) {
                    continue;
                }

                // 如果set的属性在元组属性列表中，进行赋值
                if (updateSet.getExpressions().get(0) instanceof StringValue) {
                    updateValue[i] = ((StringValue) updateSet.getExpressions().get(0)).getValue();
                } else {
                    updateValue[i] = updateSet.getExpressions().get(0).toString();
                }
                indexs[i] = j; // set语句中的第i个对应于源类中第j个属性
                break;
            }
        }
    }
}
