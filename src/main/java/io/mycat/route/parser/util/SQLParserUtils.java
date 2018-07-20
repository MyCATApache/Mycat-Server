package io.mycat.route.parser.util;

import org.junit.Assert;

import java.util.*;

public class SQLParserUtils {
	
	class State{
		Integer state = -1;
	}

	State state = new State();			//当前处于哪个部分 0: select 1: from 2:where 

    Stack<State> stateStack = new Stack<State>();

	Map<String,String> tables = new HashMap<String,String>();
	boolean tableFlag = false;		//在From部分出现关键字from join 逗号后的是表名
    
	public Map<String,String> parse(String sql){
		tables.clear();
		stateStack.clear();
		state = null;
		
	    tableFlag = false;
		
		boolean sFlag = false;			//单引号 
		boolean dFlag = false;			//双引号计数器
		Scanner reader=new Scanner(sql);
		reader.useDelimiter(" ");
		String value;
		while(reader.hasNext()){
			value = reader.next().toLowerCase();
			//前面已经出现单引号，在再次出现单引号之前不做任何处理
			if (sFlag){
				if (value.endsWith("'")&& getCount(value,"'")==1){
					sFlag = false;
					continue;
				}else if (value.indexOf("'")!=-1){
					value = value.substring(value.indexOf("'")+1);
					sFlag = false;
				}else {
					continue;
				}
			}
			//前面已经出现双引号，在再次出现双引号之前不做任何处理
			if (dFlag){
				if (value.endsWith("\"")&& getCount(value,"\"")==1){
					dFlag = false;
					continue;
				}else if (value.indexOf("\"")!=-1){
					value = value.substring(value.indexOf("\"")+1);
					dFlag = false;
				}else {
					continue;
				}
			}
			//单引号在select，where部分不做处理
			if (state != null && state.state !=1 && getCount(value,"'")%2==1){
				sFlag = true;
				continue;
			}
			if (state != null && state.state !=1 && getCount(value,"\"")%2==1){
				dFlag = true;
				continue;
			}
			
			//SELECT关键字
			if (value.equals("select") || value.equals("(select")){
				//if (state != null) 
				state = new State();
				state.state = 0;
				stateStack.push(state);	//入栈
				continue;
			}
			
			
			//FROM关键字
			if (value.equals("from") || value.equals("into")|| value.equals("join")){
				state.state = 1;
				tableFlag = true;
				continue;
			}
			//From部分出现逗号后面是表名
			if (state.state == 1 && value.equals(",")){
				tableFlag = true;
				continue;
			}
			if (state.state == 1 && tableFlag == true){
				getTableName(value);
				continue;
			}

			if (state.state == 1 && tableFlag == false){
				if (!value.startsWith("),") &&(value.equals(",")|| value.endsWith(","))){
					tableFlag = true;
					continue;
				}else if (!value.startsWith("),") && value.indexOf(",")!=-1){
					getTableName(value);
					continue;
				}
					
			}
			
			//WHERE关键字
			if (value.equals("where")){
				state.state = 2;
				continue;
			}
			
			if (value.endsWith("(select")){

				stateStack.push(state);
				state = new State();
				state.state = 0;
				continue;
			}
			if ( value.equals(")")|| value.startsWith("),")){
				stateStack.pop();
				state = stateStack.peek();
				tableFlag = value.endsWith(",")?true:false;
				if (state.state ==1){
					getTableName(value);
				}
				continue;
			}


		}
			
		return tables;
	}
	
	private void getTableName(String str){
		String[] t = str.split(",");
		for (int i=tableFlag?0:1; i<t.length;i++){
			if (t[i].endsWith(")")){
				tables.put(t[i].substring(0,t[i].length()-1), "");
				stateStack.pop();
				state = stateStack.peek();
				if (state.state != 1){
					break;
				}
			}else if (t[i].equals("(select")){
				
				state = new State();
				state.state = 0;
				stateStack.push(state);
				break;
			}else{
				if (t[i].trim().length()>0 && !t[i].trim().equals("(")) {
					tables.put(t[i], "");
				}
			}
		}
		if (!str.endsWith(",")) {
			tableFlag = false;
		}
	}
	
	
	
	public static int getCount(String str,String match){
	       int count = 0;
	        int index = 0;
	        while((index=str.indexOf(match,index))!=-1){
	            index = index+match.length();
	            count++;
	        }
	        return count;
	}
	
	
	private boolean test(String sql,String[] tables){
		

		Map<String,String> result = parse(sql);
		if (result.size() != tables.length) {
			return false;
		}
		for (String tmp : tables){
			if (result.get(tmp.toLowerCase())==null) {
				return false;
			}
		}
		return true;
			
	}
	private static final String sql1 = "select t3.*,ztd3.TypeDetailName as UseStateName\n" +
            "from\n" +
            "( \n" +
            " select t4.*,ztd4.TypeDetailName as AssistantUnitName\n" +
            " from\n" +
            " (\n" +
            "  select t2.*,ztd2.TypeDetailName as UnitName \n" +
            "  from\n" +
            "  (\n" +
            "   select t1.*,ztd1.TypeDetailName as MaterielAttributeName \n" +
            "   from \n" +
            "   (\n" +
            "    select m.*,r.RoutingName,u.username,mc.MoldClassName\n" +
            "    from dbo.D_Materiel as m\n" +
            "    left join dbo.D_Routing as r\n" +
            "    on m.RoutingID=r.RoutingID\n" +
            "    left join dbo.D_MoldClass as mc\n" +
            "    on m.MoldClassID=mc.MoldClassID\n" +
            "    left join dbo.D_User as u\n" +
            "    on u.UserId=m.AddUserID\n" +
            "   )as t1\n" +
            "   left join dbo.D_Type_Detail as ztd1 \n" +
            "   on t1.MaterielAttributeID=ztd1.TypeDetailID\n" +
            "  )as t2\n" +
            "  left join dbo.D_Type_Detail as ztd2 \n" +
            "  on t2.UnitID=ztd2.TypeDetailID\n" +
            " ) as t4\n" +
            " left join dbo.D_Type_Detail as ztd4 \n" +
            " on t4.AssistantUnitID=ztd4.TypeDetailID\n" +
            ")as t3\n" +
            "left join dbo.D_Type_Detail as ztd3 \n" +
            "on t3.UseState=ztd3.TypeDetailID";
	public static void main(String[] args) {
		SQLParserUtils parser = new SQLParserUtils();
		//parser.parse("select 'select * from C , D',' select * from E' from B");
		//if (true) return;
		List<String[]> list = new ArrayList<String[]>();
		list.add(new String[]{"select * from B","B"});
		list.add(new String[]{"select * from B,C","B,C"});
		list.add(new String[]{"select * from B ,C","B,C"});
		list.add(new String[]{"select * from B , C","B,C"});
		list.add(new String[]{"select * from B a","B"});
		list.add(new String[]{"select * from B a,C,D","B,C,D"});
		list.add(new String[]{"select * from B a,C e ,D","B,C,D"});
		list.add(new String[]{"select * from B a,C e ,D f","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C),D","B,C,D"});
		list.add(new String[]{"select * from B, (select * from C),D","B,C,D"});
		list.add(new String[]{"select * from B, ( select * from C),D","B,C,D"});
		list.add(new String[]{"select * from B, ( select * from C),D,E","B,C,D,E"});
		list.add(new String[]{"select * from B,(select * from C ),D","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C ) ,D","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C), D","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C ) , D","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C ) , (select * from D )","B,C,D"});
		list.add(new String[]{"select * from B,(select * from C ) , (select * from D ),E","B,C,D,E"});
		list.add(new String[]{"select * from B,(select C.ID , D.ID from C ) , (select * from D ),E","B,C,D,E"});

		list.add(new String[]{"select (select C.ID,D.ID from C ) from B, D","B,C,D"});
		list.add(new String[]{"select (select C.ID,D.ID from C ) , E from B, D","B,C,D"});
		list.add(new String[]{"select (select C.ID,D.ID from C ), E from B, D","B,C,D"});
		list.add(new String[]{"select (select C.ID,D.ID from C ),E from B, D","B,C,D"});
		list.add(new String[]{"select a from t1 union select b from t2","t1,t2"});
		

		list.add(new String[]{"select * from B where C =1","B"});
		list.add(new String[]{"select * from B where C = (select 1 from D)","B,D"});
		list.add(new String[]{"select * from B where C = (select 1 from D) AND E = (select 2 from F)","B,D,F"});
		
		
		list.add(new String[]{"select * from B INNER JOIN C ON C.ID = D.ID","B,C"});
		list.add(new String[]{"select * from B INNER JOIN C ON C.ID = D.ID INNER JOIN E ON 1=1","B,C,E"});
		list.add(new String[]{"select * from B INNER JOIN C ON C.ID = (select G,H FROM I) INNER JOIN E ON 1=1","B,C,E,I"});
		list.add(new String[]{"select * from B INNER JOIN C ON C.ID = (select G,H FROM I ) INNER JOIN E ON 1=1","B,C,E,I"});
		list.add(new String[]{"select * from B INNER JOIN C ON C.ID = ( select G,H FROM I ) INNER JOIN E ON 1=1","B,C,E,I"});
		
		

		list.add(new String[]{"select 'select * from C' from B","B"});
		list.add(new String[]{"select 'select * from C,D' from B","B"});
		list.add(new String[]{"select 'select * from C , D',E from B","B"});
		list.add(new String[]{"select 'select * from C , D',' select * from E' from B","B"});
		list.add(new String[]{"select 'select * from C , D','F',' select * from E' from B","B"});
		list.add(new String[]{"select 'select * from C , D',' F',' select * from E' from B","B"});
		list.add(new String[]{"select 'select * from C , D',' F ',' select * from E' from B","B"});
		

		list.add(new String[]{"select * from 'B'","'B'"});
		list.add(new String[]{"select * from 'B','C'","'B','C'"});
		
		list.add(new String[]{sql1,"dbo.D_Materiel,dbo.D_Routing,dbo.D_MoldClass,dbo.D_Type_Detail,dbo.D_User"});
		//String sql  = "select ' form \"' * from \"B\",C where a='c'";
		//String sql  = "select ' form \"' * from \"B\",C";
		for (String[] tmp :list){
			
			Assert.assertTrue(tmp[0],parser.test(tmp[0],tmp[1].split(",")));
			{
				System.out.println(tmp[0]+"--->"+tmp[1]);
				Map<String,String> tables = parser.parse(tmp[0]);
				System.out.print("表名：");
				for (String key :tables.keySet()) {
					System.out.println(key);
				}
			}
		}
		// TODO Auto-generated method stub

	}

}
