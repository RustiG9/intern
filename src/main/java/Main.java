//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.pcap4j.core.*;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.io.EOFException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.sql.*;

import java.util.*;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Main {

    private LoopPcap loopPcap = new LoopPcap();
    private boolean startStopRun = true;
    private List<PcapNetworkInterface> allDev;
    private List<PcapAddress> plistAdres = new ArrayList<>();
    private String adressDev = "";
    private int sleep = 20;

    {
        try {
            allDev = Pcaps.findAllDevs();
        } catch (PcapNativeException e) {
            e.printStackTrace();
        }
    }

    private JButton stop = new JButton("Stop");
    private JButton start = new JButton("Start");
    private JTextArea input = new JTextArea("input");
    private JTextField min = new JTextField("1024");
    private JTextField max = new JTextField("1073741824");
    private JTextField ipField = new JTextField();
    private JScrollPane scrollPaneInput = new JScrollPane();
    private JLabel label1 = new JLabel("Select an adapter");
    private JLabel label2 = new JLabel("Lenght of packet");
    private JLabel label3 = new JLabel("Total packets length");
    private JLabel label4 = new JLabel("Min value, byte");
    private JLabel label5 = new JLabel("Max value, byte");
    private JLabel label6 = new JLabel("Threshold update times, min");
    private JLabel label7 = new JLabel("Traffic verification time, min");
    private JLabel label8 = new JLabel("Set IP");
    private JSlider slider4Lable6 = new JSlider(1, 5, 5);
    private JSlider slider4Lable7 = new JSlider(10, 60, 20);
    private JComboBox adapterList = new JComboBox();
    private JRadioButton uknownIp = new JRadioButton("Uknown Ip");
    private JRadioButton knownIp = new JRadioButton("Known Ip");
    private ButtonGroup bgIp = new ButtonGroup();

    private final String DB_URL = "jdbc:postgresql://localhost:5432/traffic_limits";
    private final String USER = "username";
    private final String PASS = "password";

    private SparkStringConsumer sparkStringConsumer = new SparkStringConsumer(Integer.parseInt(min.getText()), Integer.parseInt(max.getText()), slider4Lable6.getValue());

    public class Frame extends JFrame {
        String ad = "";
        int count = 0;

        private Frame() {
            for (PcapNetworkInterface nif : allDev
            ) {
                if (nif.getAddresses().size() > 0) {
                    ad = nif.getDescription();
                    adapterList.addItem((count++) + "." + ad);
                    plistAdres.add(nif.getAddresses().get(1));
                }
            }
            input.setText("");
            initializationComponents();
        }

        private void initializationComponents() {
            setBounds(15, 30, 800, 600);
            setSize(830, 600);
            setResizable(false);
            setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            Container container = getContentPane();
            container.setLayout(null);
            container.setBounds(5, 5, 800, 600);


            // JTextArea
            input.setLineWrap(true);
            input.setColumns(20);
            input.setRows(5);
            input.setBounds(50, 220, 790, 300);
            scrollPaneInput.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
            scrollPaneInput.setBounds(5, 220, 800, 300);
            scrollPaneInput.setViewportView(input);
            container.add(scrollPaneInput);


            // JComboBox
            adapterList.setBounds(20, 25, 300, 20);
            adapterList.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    start.setEnabled(true);
                    int a = adapterList.getSelectedIndex();
                    adressDev = plistAdres.get(a).getAddress().toString().replaceAll("/", "");
                }
            });
            container.add(adapterList);


            // JLabel
            label1.setBounds(20, 5, 300, 20);
            // прозрачный фон
            label1.setOpaque(true);
            label1.setForeground(Color.red);
            container.add(label1);
            //
            label2.setBounds(20, 190, 300, 20);
            container.add(label2);
            //
            label3.setBounds(20, 530, 250, 20);
            container.add(label3);
            //
            label4.setBounds(20, 50, 250, 20);
            container.add(label4);
            //
            label5.setBounds(20, 80, 250, 20);
            container.add(label5);
            //
            label6.setBounds(40, 100, 250, 20);
            container.add(label6);
            //
            label7.setBounds(320, 100, 250, 20);
            container.add(label7);
            //
            label8.setBounds(280, 50, 250, 20);
            container.add(label8);


            //JTexField
            min.setBounds(120, 50, 100, 20);
            container.add(min);
            min.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if ("".equals(min.getText())) {
                        sparkStringConsumer.setMin(1073741824);
                    } else {
                        sparkStringConsumer.setMin(Integer.parseInt(min.getText()));
                    }

                    try {
                        Class.forName("org.postgresql.Driver");
                    } catch (ClassNotFoundException e1) {
                        e1.printStackTrace();
                    }
                    Connection connection = null;
                    try {
                        connection = DriverManager.getConnection(DB_URL, USER, PASS);
                        connection.setAutoCommit(false);
                        String sqlUmin = "INSERT INTO limits_per_hour (limit_name,limit_value,effective_date)" +
                                " VALUES ('min',(?),(?));";
                        PreparedStatement stmtInMin = connection.prepareStatement(sqlUmin);
                        stmtInMin.setInt(1, Integer.parseInt(min.getText()));
                        stmtInMin.setDate(2, new java.sql.Date(new Date().getTime()));
                        stmtInMin.executeUpdate();
                        stmtInMin.close();
                        connection.commit();
                        connection.close();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }

                }
            });
            //
            max.setBounds(120, 80, 100, 20);
            container.add(max);
            max.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if ("".equals(max.getText())) {
                        sparkStringConsumer.setMax(1024);
                    } else {
                        sparkStringConsumer.setMax(Integer.parseInt(max.getText()));
                    }

                    try {
                        Class.forName("org.postgresql.Driver");
                    } catch (ClassNotFoundException e1) {
                        e1.printStackTrace();
                    }
                    Connection connection = null;
                    try {
                        connection = DriverManager.getConnection(DB_URL, USER, PASS);
                        connection.setAutoCommit(false);
                        String sqlUmax = "INSERT INTO limits_per_hour (limit_name,limit_value,effective_date)" +
                                " VALUES ('max',(?),(?));";
                        PreparedStatement stmtInMax = connection.prepareStatement(sqlUmax);
                        stmtInMax.setInt(1, Integer.parseInt(max.getText()));
                        stmtInMax.setDate(2, new java.sql.Date(new Date().getTime()));
                        stmtInMax.executeUpdate();
                        stmtInMax.close();
                        connection.commit();
                        connection.close();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }
                }
            });
            //
            ipField.setBounds(420, 70, 150, 20);
            ipField.setVisible(false);
            container.add(ipField);

            //JRadioButton
            bgIp.add(uknownIp);
            bgIp.add(knownIp);
            uknownIp.setSelected(true);
            uknownIp.setBounds(320, 50, 100, 20);
            uknownIp.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    ipField.setVisible(false);
                    ipField.setText("");
                }
            });
            knownIp.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    ipField.setVisible(true);
                }
            });

            knownIp.setBounds(420, 50, 100, 20);
            container.add(uknownIp);
            container.add(knownIp);


            //JSlider
            slider4Lable6.setBounds(10, 120, 250, 30);
            slider4Lable6.setPaintLabels(true);
            slider4Lable6.setMajorTickSpacing(1);
            slider4Lable6.setSnapToTicks(true);
            container.add(slider4Lable6);
            //
            container.add(slider4Lable7);
            slider4Lable7.setBounds(270, 120, 250, 30);
            slider4Lable7.setPaintLabels(true);
            slider4Lable7.setMajorTickSpacing(5);
            slider4Lable7.setSnapToTicks(true);
            container.add(slider4Lable7);
            slider4Lable7.addChangeListener(new ChangeListener() {
                @Override
                public void stateChanged(ChangeEvent e) {
                    sleep = slider4Lable7.getValue();
                }
            });

            // JButton
            start.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    adapterList.setEnabled(false);
                    stop.setEnabled(true);
                    start.setEnabled(false);
                    startStopRun = true;
                    loopPcap.start();
                    sparkStringConsumer.start();
                }
            });
            start.setBounds(20, 155, 80, 25);
            start.setEnabled(false);
            container.add(start);

            stop.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    adapterList.setEnabled(true);
                    start.setEnabled(true);
                    stop.setEnabled(false);
                    startStopRun = false;
                    loopPcap = new LoopPcap();
                    sparkStringConsumer = new SparkStringConsumer(Integer.parseInt(min.getText()), Integer.parseInt(max.getText()), slider4Lable6.getValue());
                }
            });
            stop.setBounds(120, 155, 80, 25);
            stop.setEnabled(false);
            container.add(stop);
        }
    }

    private class LoopPcap extends Thread {

        @Override
        public void run() {
            int n = 0, count = 0;
            String ip = ipField.getText().trim();
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer kafkaProducer = new KafkaProducer(properties);
            try {
                InetAddress addr = InetAddress.getByName(adressDev);
                PcapNetworkInterface nif = Pcaps.getDevByAddress(addr);
                PcapHandle handle = nif.openLive(65536, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, 0);
                while (startStopRun) {
                    int i = 0;
                    sleep(2);
                    Packet packet = handle.getNextPacketEx();
                    IpV4Packet ipV4Packet = packet.get(IpV4Packet.class);
                    if (ipV4Packet != null) {
                        if (ipV4Packet.getHeader().getSrcAddr().toString().replaceAll("/", "").equals(adressDev) &&
                                ipV4Packet.getHeader().getDstAddr().toString().replaceAll("/", "").equals("185.42.205.51") ||
                                ipV4Packet.getHeader().getSrcAddr().toString().replaceAll("/", "").equals("185.42.205.51") &&
                                        ipV4Packet.getHeader().getDstAddr().toString().replaceAll("/", "").equals(adressDev)) {
                            i = ipV4Packet.getHeader().getTotalLength();
                            n += i;
                            input.setText(packet.toString());
                            label2.setText(String.format("Lenght of packet %d bytes", i));
                            label3.setText(String.format("Total packets length %d bytes", n));

                        } else if ("".equals(ip)) {
                            i = ipV4Packet.getHeader().getTotalLength();
                            n += i;
                            input.setText(packet.toString());
                            label2.setText(String.format("Lenght of packet %d bytes", i));
                            label3.setText(String.format("Total packets length %d bytes", n));
                        }
                    }
                    kafkaProducer.send(new ProducerRecord("test", Integer.toString(count++), Integer.toString(i)));
                }
                handle.close();
            } catch (UnknownHostException | InterruptedException | PcapNativeException | EOFException | TimeoutException | NotOpenException | ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                e.printStackTrace();
                kafkaProducer.close();
            } catch (KafkaException e) {
                kafkaProducer.abortTransaction();
            } finally {
                kafkaProducer.close();
            }
            interrupt();
        }
    }

    private class Jdb extends Thread {

        long firstTime = 0;
        int max, min;

        @Override
        public void run() {
            while (true) {
                try {
                    sleep(60000 * sleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    Class.forName("org.postgresql.Driver");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                Connection connection = null;
                try {
                    connection = DriverManager.getConnection(DB_URL, USER, PASS);
                    connection.setAutoCommit(false);
                    Statement stmt = connection.createStatement();

                    String sqlQ = "SELECT * FROM limits_per_hour;";
                    ResultSet rs = stmt.executeQuery(sqlQ);
                    while (rs.next()) {
                        long d = rs.getDate("effective_date").getTime();
                        if (d >= firstTime) {
                            firstTime = d;
                            String name = rs.getString("limit_name");
                            if ("min".equals(name)) {
                                min = rs.getInt("limit_value");
                            }
                            if ("max".equals(name)) {
                                max = rs.getInt("limit_value");
                            }
                        }
                    }
                    String sqlUmin = "INSERT INTO limits_per_hour (limit_name,limit_value,effective_date)" +
                            " VALUES ('min',(?),(?));";
                    String sqlUmax = "INSERT INTO limits_per_hour (limit_name,limit_value,effective_date)" +
                            " VALUES ('max',(?),(?));";
                    PreparedStatement stmtInMin = connection.prepareStatement(sqlUmin);
                    stmtInMin.setInt(1, min);
                    stmtInMin.setDate(2, new java.sql.Date(new Date().getTime()));
                    stmtInMin.executeUpdate();
                    PreparedStatement stmtInMax = connection.prepareStatement(sqlUmax);
                    stmtInMax.setInt(1, max);
                    stmtInMax.setDate(2, new java.sql.Date(new Date().getTime()));
                    stmtInMax.executeUpdate();

                    stmt.close();
                    stmtInMin.close();
                    stmtInMax.close();
                    rs.close();
                    connection.commit();
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) {
        Main t = new Main();
        Main.Frame f = t.new Frame();
        f.setVisible(true);
        Main.Jdb jdb = t.new Jdb();
        jdb.start();


    }
}
