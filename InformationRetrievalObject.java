package InformationRetrieval;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class InformationRetrievalObject implements Writable {

	private Text documentID;
	private IntWritable freq;
	private Text word;
	private IntWritable totalWordsOfDocID;
	private IntWritable allThisWord;

	public InformationRetrievalObject() {
		this.documentID = new Text();
		this.freq = new IntWritable();
		this.word = new Text();
		this.totalWordsOfDocID = new IntWritable();
		this.allThisWord = new IntWritable();
	}
	

	public void set(String docID, int freq, String word) {
		this.documentID.set(docID);
		this.freq.set(freq);
		this.word.set(word);
	}
	
	public void set2(String docID, int freq, String word, int totalWordsOfDocID) {
		this.documentID.set(docID);
		this.freq.set(freq);
		this.word.set(word);
		this.totalWordsOfDocID.set(totalWordsOfDocID);
	}
	
	public void set3(String docID, int freq, String word, int totalWordsOfDocID, int allThisWord) {
		this.documentID.set(docID);
		this.freq.set(freq);
		this.word.set(word);
		this.totalWordsOfDocID.set(totalWordsOfDocID);
		this.allThisWord.set(allThisWord);
	}


	@Override
	public void readFields(DataInput dataInput) throws IOException {
		documentID.readFields(dataInput);
		freq.readFields(dataInput);
		word.readFields(dataInput);
		totalWordsOfDocID.readFields(dataInput);
		allThisWord.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		documentID.write(dataOutput);
		freq.write(dataOutput);
		word.write(dataOutput);
		totalWordsOfDocID.write(dataOutput);
		allThisWord.write(dataOutput);
	}

	@Override
	public String toString() {
		return documentID.toString() + ":" + freq.get();
	}

	public Text getDocumentID() {
		return documentID;
	}

	public IntWritable getFreq() {
		return freq;
	}
	
	public Text getWord() {
		return word;
	}
	
	public IntWritable getTotalWordsOfDocID() {
		return totalWordsOfDocID;
	}

	public IntWritable getAllThisWord() {
		return allThisWord;
	}
	
	public double TFIDFCalculation(int fileNumbers) {
		
		//double nN=(double)freq.get()/(double)totalWordsOfDocID.get();
		//double DM=(double) (Math.log((double)fileNumbers/(double)allThisWord.get())+1);
		//double TFIDF = nN*DM;
		double n=freq.get();
		double Nn=totalWordsOfDocID.get();
		int fn=19;
		double m=allThisWord.get();
		double tf=n/Nn;
		double idf=Math.log(fn/m)+1;
		double tfidf=tf*idf;
		return tfidf;
	}

	
}