package org.tp.vente;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe MAP.
*/

// Notre classe MAP.
public class WC_Mapper extends Mapper<Object, Text, Text, DoubleWritable>
{
    private static final DoubleWritable ONE=new DoubleWritable(1);

    // La fonction MAP elle-même.
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        int id_clets = Integer.parseInt(conf.get("id_clets")) ;
        int id_valeur = Integer.parseInt(conf.get("id_valeur"));
        String[] token=value.toString().split(",");
        try {
            double occurence =Double.parseDouble(""+token[id_valeur]);
            context.write(new Text(token[id_clets]),new DoubleWritable(occurence) );
        } catch (NumberFormatException e) {
            System.out.println(token[id_valeur] + " ne peut pas être converti en double.");
        }

       /* StringTokenizer tok=new StringTokenizer(value.toString(), ",");
        while(tok.hasMoreTokens())
        {
            System.out.println(tok.nextToken());
            Text word=new Text(tok.nextToken());
            // On renvoie notre couple (clef;valeur): le mot courant suivi de la valeur 1 (définie dans la constante ONE).
            context.write(word, ONE);
        }*/
    }
}
