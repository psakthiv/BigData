package test.main;

import java.util.ArrayList;
import java.util.List;

public class PreprocessingText {

	public static void main(String[] args) {
		String staticString = "Midway through the first quarter, Virginia Tech had to call two timeouts in a row because then-freshmen \\u003ca href\\u003d\\"
				+ "'http://stats.washingtonpost.com/cfb/players.asp?id\\u003d168641\\u0026team\\u003d16\\' title\\u003d\\'stats.washingtonpost.com\\'\\u003eJarrett "
				+ "Boykin\\u003c/a\\u003e and \\u003ca href\\u003d\\'http://stats.washingtonpost.com/cfb/players.asp?id\\u003d155812\\u0026team\\u003d16\\' "
				+ "title\\u003d\\'stats.washingtonpost.com\\'\\u003eDanny Coale\\u003c/a\\u003e couldn’t seem to line up right, and 'they had those big "
				+ "eyes out there looking around,' "
				+ "Kevin Sherman, their position coach, said recently.";
	}
	
	
	private List<String> preprocessText(String completeString){
		
		List<String> processedText = new ArrayList<String>();
		
		String[] preprocessedText = completeString.split(" ");
		
		
		
		return processedText;
		
	}
}
