package snowflake.demo.samples;
import snowflake.utils.TokenizeEncryptUtils;
import java.util.HashMap;
import java.util.ArrayList;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

// FSI Transaction Generator for CDC Events
public class FSIEventStreamer extends EventStreamer {
    private static ArrayList<String> FSI_TRANSACTIONS = new ArrayList<>();
    HashMap<Integer,String[]> CLIENT_TRANSACTIONS;
    private TokenizeEncryptUtils CHELPER;
    private boolean SHOW_EVENTS=false;

    public FSIEventStreamer() throws Exception {
    }
    public FSIEventStreamer(Properties p) throws Exception {
        setProperties(p);
    }

    public void setProperties(Properties p) throws Exception {
        super.setProperties(p);
        CLIENT_TRANSACTIONS = new HashMap<>();
        String key=p.getProperty("AES_KEY");
        if(key==null) throw new Exception ("Property 'AES_KEY' is required" );
        String token=p.getProperty("TOKEN_KEY");
        if(token==null) throw new Exception ("Property 'TOKEN_KEY' is required" );
        String showevents=p.getProperty("SHOW_EVENTS");
        if(showevents!=null) SHOW_EVENTS=Boolean.parseBoolean(showevents);
        CHELPER=new TokenizeEncryptUtils(key,Integer.parseInt(token));
        if(DEBUG) System.out.println("*AES_KEY IS:  "+CHELPER.getKey());
        if(DEBUG) System.out.println("*AES_KEY-ENCODED IS:  "+CHELPER.getKeyEncoded());
        if(DEBUG) System.out.println("*TOKEN_KEY IS:  "+CHELPER.getToken());
    }

    public String getEvent(int transactionid) throws Exception {
        int customer_id = randomInt(1001, 1100); // FSI customer range 1001-1100
        long commit_epoch = Instant.now().toEpochMilli();
        String current_date = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        String[] transactionInfo = ((String) FSI_TRANSACTIONS.get(randomInt(0, FSI_TRANSACTIONS.size()))).split("\\|");
        String dbuser = USERS[randomInt(0, USERS.length)];
        String[] s = new String[6];
        String transaction_id_tokenized = null;
        String transaction_id_encrypted = null;
        String action = TRANSACTIONTYPES[randomInt(0, TRANSACTIONTYPES.length)];
        
        if (action.equals("INSERT") || CLIENT_TRANSACTIONS.size() == 0 || CLIENT_TRANSACTIONS.get(customer_id) == null) {
            action = "INSERT";
            String sti0 = String.format("%03d", transactionid);
            String sti1 = sti0.substring(sti0.length() - 3);
            String sti = String.valueOf(commit_epoch) + sti1;
            long sti_long = Long.parseLong(sti);
            transaction_id_tokenized = CHELPER.tokenize(sti_long);
            transaction_id_encrypted = CHELPER.encrypt(sti);
            
            // Generate FSI transaction data
            s[0] = transaction_id_tokenized;
            s[1] = transaction_id_encrypted;
            s[2] = transactionInfo[0]; // transaction_type
            s[3] = String.valueOf(customer_id);
            s[4] = current_date;
            
            // Generate transaction amount based on type
            double baseAmount = Double.parseDouble(transactionInfo[1]);
            boolean isAnomaly = randomInt(0, 100) < 5; // 5% anomaly rate
            if (isAnomaly) {
                s[5] = NF_AMT.format(randomDouble(25000, 75000)); // Anomaly: $25k-75k
            } else {
                int variation = (int)(baseAmount * 0.3);
                s[5] = NF_AMT.format(baseAmount + randomDouble(-variation, variation)); // Normal variation
            }
            
            CLIENT_TRANSACTIONS.put(customer_id, s);
        } else {
            s = (String[]) CLIENT_TRANSACTIONS.get(customer_id);
            transaction_id_tokenized = s[0];
            transaction_id_encrypted = s[1];
        }
        
        StringBuilder sb = new StringBuilder()
                .append("{")
                .append("\"object\":\"cdc_db_table_commit\",")
                .append("\"transaction\":{")
                .append("\"transaction_id\":" + transactionid + ",")
                .append("\"schema\":\"FSI_DEMO\",")
                .append("\"table\":\"TRANSACTIONS_TABLE\",")
                .append("\"action\":\"" + action + "\",")
                .append("\"primaryKey_tokenized\":\"" + transaction_id_tokenized + "\",")
                .append("\"dbuser\":\"" + dbuser + "\",")
                .append("\"committed_at\":" + commit_epoch + ",");

        if (action.equals("INSERT")) {
            sb.append("\"record_before\":{},");
        } else {
            sb.append("\"record_before\":{")
                    .append("\"transaction_id_encrypted\":\"" + transaction_id_encrypted + "\",")
                    .append("\"customer_id\":" + s[3] + ",")
                    .append("\"transaction_type\":\"" + s[2] + "\",")
                    .append("\"transaction_date\":\"" + s[4] + "\",")
                    .append("\"transaction_amount\":" + s[5] + ",")
                    .append("\"data_source\":\"STREAMING\"")
                    .append("},");
        }
        
        if (action.equals("DELETE")) {
            CLIENT_TRANSACTIONS.remove(customer_id);
            sb.append("\"record_after\":{}");
        } else {
            if (action.equals("UPDATE")) {
                // Update transaction amount for updates
                double currentAmount = Double.parseDouble(s[5]);
                s[5] = NF_AMT.format(currentAmount + randomDouble(-500, 500));
                CLIENT_TRANSACTIONS.replace(customer_id, s);
            }
            sb.append("\"record_after\":{")
                    .append("\"transaction_id_encrypted\":\"" + s[1] + "\",")
                    .append("\"customer_id\":" + s[3] + ",")
                    .append("\"transaction_type\":\"" + s[2] + "\",")
                    .append("\"transaction_date\":\"" + s[4] + "\",")
                    .append("\"transaction_amount\":" + s[5] + ",")
                    .append("\"data_source\":\"STREAMING\"")
                    .append("}");
        }
        sb.append("}");
        sb.append("}");
        
        if(SHOW_EVENTS) System.out.println("FSI TRANSACTION:  "+transaction_id_tokenized);
        else if (DEBUG) System.out.println(sb);
        return sb.toString();
    }

    // FSI Transaction Types and Sample Data
    String[] TRANSACTIONTYPES={"INSERT","INSERT","INSERT","UPDATE","UPDATE","DELETE"};
    String[] USERS={"alice_chen","bob_smith","carol_jones","david_kim","emma_davis","frank_miller","grace_wilson","henry_garcia","iris_brown","jack_taylor"};
    
    // FSI Transaction Types with base amounts (transaction_type|base_amount)
    private static final String FSI_TRANSACTION_STRING = 
        "leisure_payment|1800;" +
        "travel_expense|2500;" +
        "dining|350;" + 
        "entertainment|450;" +
        "fitness_wellness|280;" +
        "subscription_fee|75;" +
        "shopping|650;" +
        "luxury_purchase|3200;" +
        "hobby_expense|420;" +
        "gift_purchase|180";
    
    static {
        for (String s : FSI_TRANSACTION_STRING.split(";")) FSI_TRANSACTIONS.add(s);
    }
}
