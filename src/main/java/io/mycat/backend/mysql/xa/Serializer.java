package io.mycat.backend.mysql.xa;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class Serializer {
    private static final String PROPERTY_SEPARATOR = ",";
    private static final String QUOTE = "\"";
    private static final String END_ARRAY = "]";
    private static final String START_ARRAY = "[";
    private static final String START_OBJECT = "{";
    private static final String END_OBJECT = "}";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public String toJSON(CoordinatorLogEntry coordinatorLogEntry) {
        StringBuilder strBuilder = new StringBuilder(600);
        strBuilder.append(START_OBJECT);
        strBuilder.append(QUOTE).append("id").append(QUOTE).append(":").append(QUOTE).append(coordinatorLogEntry.id).append(QUOTE);
        strBuilder.append(PROPERTY_SEPARATOR);
        strBuilder.append(QUOTE).append("createTime").append(QUOTE).append(":").append(QUOTE).append(coordinatorLogEntry.createTime).append(QUOTE);
        strBuilder.append(PROPERTY_SEPARATOR);
        //strBuilder.append(QUOTE).append("wasCommitted").append(QUOTE).append(":").append(coordinatorLogEntry.wasCommitted);
        //strBuilder.append(PROPERTY_SEPARATOR);

        String prefix = "";
        if(coordinatorLogEntry.participants.length>0){
            strBuilder.append(QUOTE).append("participants").append(QUOTE);
            strBuilder.append(":");
            strBuilder.append(START_ARRAY);

            for(ParticipantLogEntry participantLogEntry :coordinatorLogEntry.participants){
                if(participantLogEntry==null){continue;}
                strBuilder.append(prefix);
                prefix = PROPERTY_SEPARATOR;
                strBuilder.append(START_OBJECT);
                strBuilder.append(QUOTE).append("uri").append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.uri).append(QUOTE);
                strBuilder.append(PROPERTY_SEPARATOR);
                strBuilder.append(QUOTE).append("state").append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.txState).append(QUOTE);
                strBuilder.append(PROPERTY_SEPARATOR);
                strBuilder.append(QUOTE).append("expires").append(QUOTE).append(":").append(participantLogEntry.expires);
                if (participantLogEntry.resourceName!=null) {
                    strBuilder.append(PROPERTY_SEPARATOR);
                    strBuilder.append(QUOTE).append("resourceName").append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.resourceName).append(QUOTE);
                }
                strBuilder.append(END_OBJECT);
            }
//            for (ParticipantLogEntry participantLogEntry : coordinatorLogEntry.participants) {
//
//            }
            strBuilder.append(END_ARRAY);
        }
        strBuilder.append(END_OBJECT);
        strBuilder.append(LINE_SEPARATOR);
        return strBuilder.toString();
    }
}
