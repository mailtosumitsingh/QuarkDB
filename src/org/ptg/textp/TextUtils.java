package org.ptg.textp;

import java.util.ArrayList;
import java.util.List;

public class TextUtils {
	public static List<String> getWords(String text) {
		List<String> all_Words_List = new ArrayList<String>();
		StringBuilder word = new StringBuilder();
		char[] chars = text.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			char charAt = chars[i];
			if (Character.isAlphabetic(charAt) || Character.isDigit(charAt)) {
				word.append(charAt);
			} else {
				if (word.length() > 0) {
					all_Words_List.add(word.toString());
					word = new StringBuilder();
				}
			}

		}

		return all_Words_List;
	}

	public static void main(String[] args) {
		TextUtils t = new TextUtils();
		System.out.println(t.getWords("this looks . ##$$ lik9 working () someeting [pop]"));
	}
}
