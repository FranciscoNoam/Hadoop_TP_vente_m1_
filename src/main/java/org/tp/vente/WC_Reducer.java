package org.tp.vente;

/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountReduce.java: classe REDUCE.
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE - templatee avec un type generique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class WC_Reducer  extends Reducer<Text, DoubleWritable, Text, Text>
{
    // La fonction REDUCE elle-même. Les arguments: la clef key (d'un type générique K), un Iterable de toutes les valeurs
    // qui sont associées à la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le résultat à Hadoop).
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        // Pour parcourir toutes les valeurs associées à la clef fournie.
        Iterator<DoubleWritable> i=values.iterator();
        double count=0;
        while(i.hasNext())   // Pour chaque valeur...
            count+=i.next().get();    // ...on l'ajoute au total.
        // On renvoie le couple (clef;valeur) constitué de notre clef key et du total, au format Text.
       context.write(key, new Text(count+" occurences."));
    }
}