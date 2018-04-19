package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String KEY_FIELD = "key";
    Context contextdb ;
    static final String VALUE_FIELD = "value";
    private SQLiteDatabase db , querydb;
    static final String DATABASE_NAME = "messageDB";
    static final String TABLE_NAME = "Messages";
    static final int DATABASE_VERSION = 1;
    static final String CREATE_DB_TABLE =  "CREATE TABLE "
            + TABLE_NAME + " ("
            + KEY_FIELD + " TEXT, " +
            VALUE_FIELD + " TEXT , " +
            "UNIQUE(" + KEY_FIELD + ") ON CONFLICT REPLACE);";

    static final String TAG =" SimpleDHT";
    static final String[] REMOTE_PORT0 = {"11108","11112","11116","11120","11124"};
    ContentResolver cResolver ;
    int messageSeq =0 ;
    String myPort ="";
    //@Override

    //hashMap to store hashkeys and port
    HashMap<String,String>  hashMap = new HashMap<String, String>();

    //Arraylist to store current number of node in chord
    ArrayList<String> chordlist = new ArrayList<String>();

    String my_pred ="";
    String my_succ ="";

    String delim=":" ;
    String myHashKey="" ;


    HashMap<String,Socket> hm = new HashMap<String, Socket>();





    static final int SERVER_PORT = 10000;
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        System.out.println("INSIDE DELETE FUNCTION");
        Context context = getContext();
        dbHelper dbHelper = new dbHelper(context);
        db = dbHelper.getWritableDatabase();
        String[] sele_parameter={selection};


        if(selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*") )
        {
            db.delete(TABLE_NAME,null,null);

        }

        {
            db.delete(TABLE_NAME,"key=?",sele_parameter);

        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        // TODO Auto-generated method stub
        Context context = getContext();
        System.out.println(values);
        dbHelper dbHelper = new dbHelper(context);
        db = dbHelper.getWritableDatabase();
        System.out.println("key t"+values.get("key").toString()+ " value  "+values.get("key").toString());
        String newkeyhash="";
        try {
            newkeyhash= genHash(values.get("key").toString());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(false) {
            long rowID = db.insert(TABLE_NAME, "", values);
            if (rowID > 0) {
                Uri _uri = ContentUris.withAppendedId(uri, rowID);
                getContext().getContentResolver().notifyChange(_uri, null);

                System.out.println(rowID + "  " + values);
                return _uri;
            }
            // else throw new SQLiteException("Failed to add a record into " + uri);
            db.close();
            Log.v("insert", values.toString());
            return uri;
        }else {

            // 1) check for boundary condition i.e pred>current
            if((my_pred.compareTo(myHashKey) > 0 && my_pred.compareTo(newkeyhash) >0 && myHashKey.compareTo(newkeyhash)> 0)
                    ||
                    (my_pred.compareTo(myHashKey) > 0 && newkeyhash.compareTo(my_pred) >0 && newkeyhash.compareTo(myHashKey)> 0 )
                    ||
                    newkeyhash.compareTo(my_pred) >0 && myHashKey.compareTo(newkeyhash) > 0

                    ||
                    ( my_succ.compareTo(newkeyhash)>0 && my_pred.compareTo(newkeyhash)>0 && my_pred.compareTo(myHashKey)>0 && newkeyhash.compareTo(myHashKey)<0) || myHashKey.compareTo(my_succ) == 0)
            {


                long rowID = db.insert(TABLE_NAME, "", values);
                if (rowID > 0) {
                    Uri _uri = ContentUris.withAppendedId(uri, rowID);
                    getContext().getContentResolver().notifyChange(_uri, null);

                    System.out.println(rowID + "  " + values);
                    return _uri;
                }
                // else throw new SQLiteException("Failed to add a record into " + uri);
                db.close();
                Log.v("insert", values.toString());
                System.out.println("local insert()" + myHashKey +"  " +values);
                return uri;


            }

            else {


                System.out.println("sending to " + myHashKey +"  " +values.toString() + " "+my_succ);
                StringBuilder msg = new StringBuilder();
                msg.append("INSERT");
                msg.append(delim);
                msg.append(myHashKey);
                msg.append(delim);
                msg.append(values.get("key").toString());
                msg.append(delim);
                msg.append(values.get("value").toString());

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), myPort);



            }



        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        Context context = getContext();
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            myHashKey = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        my_succ=myHashKey;

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html *//*
*/
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            System.out.println("intiating server task");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             **/
            Log.e(TAG, "Can't create a ServerSocket");

        }

        for(int i = 0 ;i <REMOTE_PORT0.length ;i++)
        {
            String hashkey="";
            try {
                int a = Integer.parseInt(REMOTE_PORT0[i])/2;
                hashkey = genHash(String.valueOf(a));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            hashMap.put(hashkey ,REMOTE_PORT0[i]) ;
            System.out.println("178   " + hashkey + " " + REMOTE_PORT0[i]);

        }


        if(!portStr.equals("5554"))
        {
            System.out.println("SEND NODE JOIN REQUEST");
            StringBuilder msg= new StringBuilder();
            msg.append("JOIN");
            msg.append(delim);
            msg.append(myPort);
            msg.append(delim);
            msg.append(portStr);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), myPort);


        }else
        {
            System.out.println("I AM 5554") ;

            try {
                chordlist.add(genHash("5554")) ;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }


        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        Context context = getContext();
        System.out.println("295 "+context);
        // contextdb=context;
        dbHelper dbHelper = new dbHelper(context);
        querydb = dbHelper.getReadableDatabase();
        System.out.println("selection 159" +selection);


        Cursor cursor =null;



        if(selection.equalsIgnoreCase("*"))
        {

            if(myHashKey.equals(my_succ)) {

                String sql = "SELECT * FROM " + TABLE_NAME;
                cursor = querydb.rawQuery(sql, null);
                return cursor ;
            }
            else {

                String sql1 = "SELECT * FROM " + TABLE_NAME;
                  cursor = querydb.rawQuery(sql1, null);



                String finalResult1 ="";
                System.out.println("GET all query rcvd sending to" +my_succ);
                System.out.println("number of row "+ cursor.getCount());



                System.out.println(finalResult1);

                StringBuilder msg = new StringBuilder();
                msg.append("GETALL");
                msg.append(delim);
                msg.append(myHashKey); //origin emulator

                String p="";

                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(hashMap.get(my_succ)));


                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    System.out.println(msg.toString());
                    out.println(msg.toString());
                    out.flush();

                    BufferedReader in;
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    System.out.println("waiting for query function ");
                    p = in.readLine();
                    if (p != null) {

                        finalResult1 = finalResult1 + p ;


                    } else {
                        System.out.println("lin e947");
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                }

                String[] resarray = p.split(delim);

                // Create a MatrixCursor filled with the rows you want to add.
                MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });

                for(int i=0;i<resarray.length-1 ;i=i+2) {
                    matrixCursor.addRow(new Object[]{resarray[i], resarray[i + 1]});

                    System.out.println(resarray[i]+"  " + resarray[i + 1]);
                }
                MergeCursor mergeCursor = new MergeCursor(new Cursor[] { matrixCursor, cursor });
                System.out.println("number of row  returned"+ mergeCursor.getCount());
                return mergeCursor;


            }
            // return cursor;

        }
        else if(selection.equalsIgnoreCase("@"))
        {
            String sql = "SELECT * FROM " + TABLE_NAME ;
            cursor = querydb.rawQuery(sql, null);
            return cursor ;

        }
        else
        {


            String newkeyhash="";
            try {
                newkeyhash= genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            String sql1 = "SELECT * FROM " + TABLE_NAME ;
            Cursor  cursor1 = querydb.rawQuery(sql1, null);

            System.out.println(cursor1);
            if((my_pred.compareTo(myHashKey) > 0 && my_pred.compareTo(newkeyhash) >0 && myHashKey.compareTo(newkeyhash)> 0)
                    ||
                    (my_pred.compareTo(myHashKey) > 0 && newkeyhash.compareTo(my_pred) >0 && newkeyhash.compareTo(myHashKey)> 0 )
                    ||
                    newkeyhash.compareTo(my_pred) >0 && myHashKey.compareTo(newkeyhash) > 0

                    ||
                    ( my_succ.compareTo(newkeyhash)>0 && my_pred.compareTo(newkeyhash)>0 && my_pred.compareTo(myHashKey)>0 &&
                            newkeyhash.compareTo(myHashKey)<0) || myHashKey.compareTo(my_succ) == 0) {

                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE " + KEY_FIELD + "=\""
                        + selection + "\"";
                cursor = querydb.rawQuery(sql, null);
                return cursor ;

            }
            else
            {
                //send it to successor :

                System.out.println("sending to " + my_succ +"  " +"for " + " "+newkeyhash);
                StringBuilder msg = new StringBuilder();
                msg.append("QUERYONCE");
                msg.append(delim);
                msg.append(myHashKey); //origin emulator
                msg.append(delim);
                msg.append(selection);
                msg.append(delim);

                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(hashMap.get(my_succ)));


                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    System.out.println(msg.toString());
                    out.println(msg.toString());
                    out.flush();

                    BufferedReader in;
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    String p = in.readLine();
                    if (p != null) {

                        // return p;
                        String[] resarray = p.split(delim);
                        // Create a MatrixCursor filled with the rows you want to add.
                        MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });
                        matrixCursor.addRow(new Object[] { resarray[0], resarray[1] });

                        return matrixCursor;


                    } else {
                        System.out.println("lin e947");
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public  Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public class  dbHelper extends SQLiteOpenHelper {


        dbHelper(Context context) {
            super(context, DATABASE_NAME, null, 1);
            contextdb=context;
        }
        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_DB_TABLE);




        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }







    }

    private class ServerTask extends AsyncTask<ServerSocket, String, String> {

        @Override
        protected String doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            System.out.println(serverSocket);

            String Message_Recived ;

            Socket sock =null ;


            try {


                for(;;) {
                    // System.out.println("sock 365 " + sock);

                    try {
                        sock = serverSocket.accept();
                    }
                    catch (Exception e)
                    {

                        System.out.println("369  " + e);
                    }
                    BufferedReader in;

                    in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    PrintWriter out = new PrintWriter(sock.getOutputStream(), true);


                    if ((Message_Recived = in.readLine()) != null) {
                        String[] s = Message_Recived.split(":");



                        if (s[0].equals("JOIN")) {
                            //calculate hashkey
                            String hkey ="";
                            try {
                                hkey = genHash(s[2]);
                            } catch (NoSuchAlgorithmException e) {
                                System.out.println("379 ");
                                e.printStackTrace();
                            }
                            hm.put(hkey,sock);
                            chordlist.add(hkey) ;
                            if(chordlist.size()==2)
                            {
                                my_pred=hkey;
                                my_succ=hkey ;
                                String hkey1 ="";
                                try {
                                    hkey1 = genHash("5554");
                                } catch (NoSuchAlgorithmException e) {
                                    e.printStackTrace();
                                }
                                StringBuilder msg  = new StringBuilder();
                                msg.append("JOINSUCCESS") ;
                                msg.append(delim);
                                msg.append(hkey1);
                                msg.append(delim);
                                msg.append(hkey1);
                                System.out.println("397 two chord  "+msg.toString());

                                out.println(msg.toString());
                                out.flush();
                            }
                            else
                            {
                                Collections.sort(chordlist);
                                // Collections.reverse(chordlist);
                                String yourpred="" ;
                                String yoursucc ="" ;
                                for(int i = 0 ;i<chordlist.size(); i++)
                                {
                                    if(hkey.equals(chordlist.get(i)))
                                    {
                                        if(i==0)
                                        {
                                            yourpred = chordlist.get(chordlist.size()-1);
                                            yoursucc=chordlist.get(1);

                                        }
                                        else if(i==chordlist.size()-1)
                                        {
                                            yourpred = chordlist.get(i-1);
                                            yoursucc=chordlist.get(0);

                                        }
                                        else
                                        {

                                            yourpred = chordlist.get(i-1);
                                            yoursucc=chordlist.get(i+1);

                                        }
                                    }

                                }
                                System.out.println("pred/succ for  "+hkey +" " +yourpred + " "+yoursucc) ;
                                StringBuilder msg  = new StringBuilder();
                                msg.append("JOINSUCCESS") ;
                                msg.append(delim);
                                msg.append(yourpred);
                                msg.append(delim);
                                msg.append(yoursucc);
                                System.out.println("397 else  "+msg.toString());
                                out.println(msg.toString());
                                out.flush();

                                // update others about the new pred/succ/status



                                for(int j=0 ; j<chordlist.size() ;j++)
                                {
                                    System.out.println(chordlist.get(j)+ " "+ j);

                                    if(chordlist.get(j).equals(hkey) || chordlist.get(j).equals(myHashKey) )
                                    {

                                        System.out.println(chordlist.get(j)+ " "+ j);


                                        if(chordlist.get(j).equals(myHashKey))
                                        {

                                            if(j==0)
                                            {
                                                my_pred = chordlist.get(chordlist.size()-1);
                                                my_succ=chordlist.get(1);

                                            }
                                            else if(j==chordlist.size()-1)
                                            {
                                                my_pred = chordlist.get(j-1);
                                                my_succ=chordlist.get(0);

                                            }
                                            else
                                            {

                                                my_pred = chordlist.get(j-1);
                                                my_succ=chordlist.get(j+1);

                                            }
                                        }
                                        else{

                                            System.out.println("wooooh");
                                        }

                                    }
                                    else
                                    {


                                        System.out.println("updating for "+ chordlist.get(j)+"socket" +hm.get(chordlist.get(j)) );

                                        yourpred="" ;
                                        yoursucc ="" ;
                                        for(int i = 0 ;i<chordlist.size(); i++)
                                        {
                                            if(chordlist.get(j).equals(chordlist.get(i)))
                                            {
                                                if(i==0)
                                                {
                                                    yourpred = chordlist.get(chordlist.size()-1);
                                                    yoursucc=chordlist.get(1);

                                                }
                                                else if(i==chordlist.size()-1)
                                                {
                                                    yourpred = chordlist.get(i-1);
                                                    yoursucc=chordlist.get(0);

                                                }
                                                else
                                                {

                                                    yourpred = chordlist.get(i-1);
                                                    yoursucc=chordlist.get(i+1);

                                                }
                                            }

                                        }
                                        // System.out.println("pred/succ for  "+hkey +" " +yourpred + " "+yoursucc) ;
                                        StringBuilder msg1  = new StringBuilder();
                                        msg1.append("updatedpredsucc") ;
                                        msg1.append(delim);
                                        msg1.append(yourpred);
                                        msg1.append(delim);
                                        msg1.append(yoursucc);
                                        msg1.append(delim);
                                        msg1.append(chordlist.get(j));


                                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1.toString(), myPort);



                                    }

                                }//end for j



                            }//more then two chord

                            for(int i = 0 ;i<chordlist.size(); i++) {

                                //System.out.print(chordlist.get(i)+" ") ;


                            }

                            System.out.println("5554 pred succ"+my_pred +"  " +my_succ);

                        }//end of msg[0]=JOIN
                        else if(s[0].equals("updatedpredsucc"))
                        {
                            System.out.println("UPDATING PRED SUCC RECORD ");
                            my_succ=s[2];
                            my_pred=s[1];

                            System.out.println("pred succ for "+myHashKey +" " +my_pred +" "+my_succ);

                        }else if(s[0].equals("INSERT"))
                        {
                            String hashKey="" ;
                            try {
                                hashKey = genHash(s[2]) ;
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }

                            System.out.println("insert locally " +s[2] + " "+s[3]);
                            ContentValues values = new ContentValues() ;
                            values.put("key" , s[2]);
                            values.put("value" , s[3]);
                            System.out.println("local insert " + myHashKey +"  " +values);
                            System.out.println(mUri);
                            insert(mUri,values);



                        }else if(s[0].equals("QUERYONCE"))
                        {


                            System.out.println("if");

                            // query(mUri,null,s[1],null,null);
                            String newkeyhash="";
                            try {
                                newkeyhash= genHash(s[2]);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }

                            if((my_pred.compareTo(myHashKey) > 0 && my_pred.compareTo(newkeyhash) >0 && myHashKey.compareTo(newkeyhash)> 0)
                                    ||
                                    (my_pred.compareTo(myHashKey) > 0 && newkeyhash.compareTo(my_pred) >0 && newkeyhash.compareTo(myHashKey)> 0 )
                                    ||
                                    newkeyhash.compareTo(my_pred) >0 && myHashKey.compareTo(newkeyhash) > 0

                                    ||
                                    ( my_succ.compareTo(newkeyhash)>0 && my_pred.compareTo(newkeyhash)>0 && my_pred.compareTo(myHashKey)>0 &&
                                            newkeyhash.compareTo(myHashKey)<0)  || myHashKey.compareTo(my_succ) == 0) {


                                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE " + KEY_FIELD + "=\""
                                        + s[2] + "\"";
                                System.out.println("796 " + sql);
                                Context context = getContext();
                                //System.out.println(context);
                                dbHelper dbHelper = new dbHelper(context);
                                SQLiteDatabase querydb1 = dbHelper.getReadableDatabase();

                                Cursor resultCursor = querydb1.rawQuery(sql, null);

                                String returnKey ="";
                                String returnValue="";
                                if (resultCursor.moveToFirst())
                                {

                                    int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                                    int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

                                    returnKey = resultCursor.getString(keyIndex);
                                    returnValue = resultCursor.getString(valueIndex);
                                }
                                System.out.println("804");
                                StringBuilder s1 = new StringBuilder();
                                s1.append(returnKey);
                                s1.append(delim);
                                s1.append(returnValue);


                                PrintWriter out1 = new PrintWriter(sock.getOutputStream(), true);
                                out1.println(s1.toString());
                                System.out.println(s1.toString());
                                out1.flush();



                                //return  s1.toString();
                            }
                            else
                            {

                                //send to successor
                                if(!s[1].equals(my_succ))
                                {
                                    Socket socket = null;
                                    try {
                                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(hashMap.get(my_succ)));


                                        PrintWriter out2 =
                                                new PrintWriter(socket.getOutputStream(), true);
                                        System.out.println(Message_Recived);
                                        out2.println(Message_Recived);
                                        out2.flush();

                                        BufferedReader inn;
                                        inn = new BufferedReader(new InputStreamReader(socket.getInputStream()));


                                        String p = inn.readLine();
                                        if (p != null) {


                                            PrintWriter out1 = new PrintWriter(sock.getOutputStream(), true);
                                            out1.println(p);
                                            //   System.out.println(s1.toString());
                                            out1.flush();
                                            // return p;


                                        } else {

                                        }




                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                }

                            }

                        }else if(s[0].equals("GETALL"))
                        {
                            String sql = "SELECT * FROM " + TABLE_NAME ;

                            System.out.println("Query RCVD frrom " + my_pred);
                            Context context = getContext();
                            //System.out.println(context);
                            dbHelper dbHelper = new dbHelper(context);
                            SQLiteDatabase querydb1 = dbHelper.getReadableDatabase();

                            Cursor resultCursor = querydb1.rawQuery(sql, null);


                            String finalResult ="";
                            System.out.println("number of rows " + resultCursor.getCount());


                            if (resultCursor != null) {
                               
                                if (resultCursor.moveToFirst()) {

                                    do {

                                        String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                                        String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                                        finalResult = finalResult + key + ":" + value + ":";


                                    } while (resultCursor.moveToNext());
                                }
                            }
                                    System.out.println("my rows   "+ finalResult);
                            System.out.println("Now Sending query to sucessor " +my_succ);

                            if(!s[1].equals(my_succ))
                            {
                                Socket socket = null;
                                try {
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(hashMap.get(my_succ)));


                                    PrintWriter out2 =
                                            new PrintWriter(socket.getOutputStream(), true);
                                    System.out.println(Message_Recived);
                                    out2.println(Message_Recived);
                                    out2.flush();

                                    BufferedReader inn;
                                    inn = new BufferedReader(new InputStreamReader(socket.getInputStream()));


                                    String p = inn.readLine();
                                    if (p != null) {
                                        System.out.println("Succ row recvd    "+ my_succ+" " + p );

                                        //code to sent final result
                                        finalResult=finalResult + p;
                                        System.out.println(finalResult);
                                        PrintWriter out1 = new PrintWriter(sock.getOutputStream(), true);
                                        out1.println(finalResult);
                                        //  System.out.println(s1.toString());
                                        out1.flush();


                                    } else {

                                    }




                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                            }
                            else
                            {
                                System.out.println("am last node " +my_pred);
                                System.out.println("finalResult"+finalResult);
                                PrintWriter out1 = new PrintWriter(sock.getOutputStream(), true);
                                out1.println(finalResult);
                                //  System.out.println(s1.toString());
                                out1.flush();

                            }






                        }

                        else
                        {


                        }


                    }//propposed ID send







                }

            } catch (IOException e) {
                Log.e(TAG, "not able to accept request  ");
            }






            return null;


        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
//
            String string = strReceived + "\n";
//

            try {
//
                ContentValues values = new ContentValues() ;
                values.put("key" ,Integer.toString(messageSeq));
                values.put("value" ,string);
                Log.e(TAG , Integer.toString(messageSeq));
                Log.e(TAG,strReceived);
                Log.e(TAG, String.valueOf(mUri));

                cResolver.insert(mUri,values);

                messageSeq++;
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;

        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            try {

                String[] msg_send = msgs[0].split(delim) ;

                System.out.println("533   " +msgs[0]);

                if(msg_send[0].equals("JOIN")) {


                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));

                    BufferedReader in;
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgs[0]);
                    out.flush();


                    String p= in.readLine() ;
                    if(p!=null)
                    {
                        System.out.println("473   "+p) ;
                        String[] p1 = p.split(":");

                        my_pred = p1[1];
                        my_succ=p1[2];

                        System.out.println(my_pred +"  553  " +my_succ);


                    }
                    else
                    {
                        System.out.println("481 no repsonse pred/succ ");
                    }


                }
                else if(msg_send[0].equals("updatedpredsucc"))
                {
                    System.out.println("sending udated  predessor and success ");
                    String rcvport = hashMap.get(msg_send[3]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(rcvport));

                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgs[0]);
                    out.flush();



                }else if(msg_send[0].equals("INSERT"))
                {

                    System.out.println("798 client sending to succ ");
                    String rcvport = hashMap.get(my_succ);
                    System.out.println(rcvport);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(rcvport));

                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgs[0]);
                    out.flush();


                }else if(msg_send[0].equals("QUERYONCE"))
                {

                    System.out.println("sending query to  " + my_succ);

                    String rcvport = hashMap.get(my_succ);


                    // if(!my_succ.equals(msg_send[1])) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(rcvport));

                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    System.out.println(msg_send);
                    out.println(msgs[0]);
                    out.flush();

                    BufferedReader in;
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    System.out.println("waiting for reply 991");
                    String p = in.readLine();
                    if (p != null) {
                        System.out.println("939 key FOUND  " + p);
                        return p;


                    } else {
                        System.out.println("lin e947");
                    }




                }//end else if
                else
                {

                    System.out.println(" 469 ");
                }
                //socket.close();
            } catch(UnknownHostException e){
                System.out.println("493  ClientTask UnknownHostException");
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }


}