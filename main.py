# -*- coding: utf-8 -*-

import re
from bs4 import BeautifulSoup
from requests import get
import csv
import pandas as pd
import numpy as np
from google.cloud import translate
import time
translate_client = translate.Client()

"""
with open("dioce_data.csv", "rt") as csvfile:
	reader = csv.reader(csvfile)
	for row in reader:
		import pdb; pdb.set_trace()
		link = "https://de.wikipedia.org/wiki/" 

""
This only works for wikis with nicely formatted tables. 
Hmm, I think what I might do si that I might jsut cooy paste this into a spreadsheet. 
Then I'll do this
"""

def get_information_v1(configeration, bishops, orig_url, index):
	if index == "":
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&format=json").json()
	else:
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&section="+index+"&format=json").json()
	html_text = BeautifulSoup( section_response["parse"]["text"]["*"])
	columns = html_text.find_all("th")
	if len(columns) == 0:
		columns = html_text.find_all("b")
	num_columns = len(columns)
	rows = html_text.find_all("tr")
	for row in rows:
		try:
			cells = row.find_all("td")
			if len(cells) < num_columns:
				continue
			cleanr = re.compile('<.*?>')
			name = re.sub(cleanr, '', str(cells[1]))
			if "</a" in name:
				name = name.replace("</a", "")
			url =re.search(r'href=[\'"]?([^\'" >]+)', str(cells[1])).group(0).replace("href=", "")
			if url is not None:
				url = "https://de.wikipedia.org/"+ url
			from_to = str(cells[2]).replace("<td>", "").replace("</td>", "").split('\xe2\x80\x93')
			from_val = from_to[0]
			to_val = from_to[1]
			bishops.loc[len(bishops)] = [name, from_val, to_val, "", str(url), str(configeration),orig_url]
		except:
			continue
	return bishops


def get_information_v2(configeration, bishops, orig_url, index, from_key, to_key, name_key):
	if index == "":
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&format=json").json()
	else:
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&section="+index+"&format=json").json()
	html_text = BeautifulSoup(section_response["parse"]["text"]["*"])
	columns = html_text.find_all("th")
	if len(columns) == 0:
		table = html_text.find_all("table")
		columns =  BeautifulSoup(str(table)).find_all("b")
	num_columns = len(columns)
	from_index = 1
	to_index = 2
	name_index = 0
	for i in range(len(columns)):
		if from_key in str(columns[i]):
			from_index = i
		if to_key in str(columns[i]):
			to_index = i
		if name_key in str(columns[i]) or "Bischof" in str(columns[i]):
			name_index = i
	rows = html_text.find_all("tr")
	try:
		for row in rows:
			cells = row.find_all("td")
			if len(cells) < num_columns or "<b>" in str(row):
				continue
			cleanr = re.compile('<.*?>')
			name = re.sub(cleanr, '', str(cells[name_index]))
			url = re.search(r'href=[\'"]?([^\'" >]+)', str(cells[0]))
			if url is not None:
				url = url.group(0).replace("href=", "")
			if url is not None:
				url = "https://de.wikipedia.org/" +url
			from_val= str(cells[from_index])
			from_val = re.sub(cleanr, '', str(from_val))
			to_val = str(cells[to_index]).replace("<td>", "")
			to_val = re.sub(cleanr, '', str(to_val))
			bishops.loc[len(bishops)] = [name, from_val, to_val, "", url, str(configeration), orig_url]
	except:
		pass
	return bishops

def initial_bishop_scrape():
	"""
	Initial bishop scrape, this will get the names, year in, yea rout (but iwthout clenaing), and the 
	links to each bishops' wikipedia page. 

	"""
	#urls = [ "https://de.wikipedia.org/wiki/Liste_der_%C3%84bte_und_Bisch%C3%B6fe_von_Fulda", "https://de.wikipedia.org/wiki/Liste_der_Bisch%C3%B6fe_von_Aachen",]
	urls = ["Liste_der_Bischöfe_von_Aachen", "Liste_der_Bischöfe_von_Mainz",  "Liste_der_Äbte_und_Bischöfe_von_Fulda","Liste_der_Bisch%C3%B6fe_von_Augsburg","Liste_der_Erzbisch%C3%B6fe_und_Bisch%C3%B6fe_von_Bamberg", "Liste_der_Bisch%C3%B6fe_und_Erzbisch%C3%B6fe_von_Berlin", "Liste_der_Bisch%C3%B6fe_von_Cammin", "Liste_der_Bischöfe_von_Dresden-Meißen", "Liste_der_Bischöfe_von_Eichstätt", "Liste_der_Bisch%C3%B6fe_von_Freising_und_der_Erzbisch%C3%B6fe_von_M%C3%BCnchen_und_Freising", "Liste_der_Bischöfe_von_Hildesheim", "Liste_der_Erzbischöfe_und_Bischöfe_von_Köln", "Liste_der_Bischöfe_von_Limburg", 
	"Liste_der_Erzbisch%C3%B6fe_und_Bisch%C3%B6fe_von_Magdeburg", "Liste_der_Bisch%C3%B6fe_von_Minden", "Liste_der_Bisch%C3%B6fe_von_M%C3%BCnster", "Liste_der_Bisch%C3%B6fe_von_Osnabr%C3%BCck", "Liste_der_Bisch%C3%B6fe_und_Erzbisch%C3%B6fe_von_Paderborn", "Liste_der_Bisch%C3%B6fe_von_Passau", "Liste_der_Bisch%C3%B6fe_von_Regensburg", "Liste_der_Bisch%C3%B6fe_von_Rottenburg-Stuttgart", "Liste_der_Bisch%C3%B6fe_von_Schleswig", "Liste_der_Bisch%C3%B6fe_von_Speyer", "Liste_der_Bisch%C3%B6fe_von_Trier", "Liste_der_Bisch%C3%B6fe_von_W%C3%BCrzburg", "Liste_der_Bisch%C3%B6fe_von_Feldkirch", 
	"Liste_der_Bisch%C3%B6fe_von_Graz-Seckau", "Liste_der_Bisch%C3%B6fe_von_Gurk-Klagenfurt", "Liste_der_Erzbisch%C3%B6fe_von_Salzburg", "Liste_der_Bisch%C3%B6fe_und_Erzbisch%C3%B6fe_von_Wien", "Liste_der_Bisch%C3%B6fe_von_St._Gallen", "Liste_der_Bisch%C3%B6fe_von_Sitten", "Liste_der_Bisch%C3%B6fe_von_Chur", "Liste_der_Bisch%C3%B6fe_von_Basel"]
	#urls = ["Liste_der_Bisch%C3%B6fe_von_M%C3%BCnster"]
	# Someitmes its von, sometimes its bis OR Amtszeit	
	# "Liste_der_Äbte_und_Bischöfe_von_Fulda", 
	## These aref or the ones that make sense. 
	# honestl this would be better. 
	failed = []
	bishops = pd.DataFrame(columns=["name", "from" , "to", "biography", "url", "bishop_type","orig_url_page"])
	for url in urls:
		response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&prop=sections&format=json").json()
		try:
			sections = response["parse"]["sections"]
			for section in sections:
				if section["line"] == u'Bisch\xf6fe' or  section["line"] == u'Bisch\xc3\xb6fe' or "Bisch\xf6fe" in  section["line"].encode('utf-8') or "Bisch\xc3\xb6fe" in section["line"].encode('utf-8') :
					index = str(section["index"])
					configeration = 0
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index+"&format=json").json()
					html_text = BeautifulSoup(section_response["parse"]["text"]["*"])
					columns = html_text.find_all("th")
					if len(columns) == 0:
						table = html_text.find_all("table")
						columns =  BeautifulSoup(str(table)).find_all("b")
					for col in columns:
						col = str(col)
						if "Amtszeit" in col:
							bishops = get_information_v1(configeration, bishops, url, index)
						elif  "Amtsantritt" in col:
							bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
						elif "bis" in col:
							bishops = get_information_v2(configeration, bishops, url, index, "von", "bis" ,"Name")
				elif section["line"] == u'F\u00fcrstbisch\u00f6fe':
					index = str(section["index"])
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index)
					configeration = 2
					if "Amtszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index)
					elif "Amtsantritt" in section_response.text:
						bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
					else:  # von bis
						bishops = get_information_v2(configeration, bishops, url, index, "von", "bis", "Name")
				elif "Erzbisch\xf6fe" in  section["line"].encode('utf-8') or "Erzbisch\xc3\xb6fe" in section["line"].encode('utf-8'):
					index = str(section["index"])
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index)
					configeration = 1
					if "Amtszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index)
					elif "Amtsantritt" in section_response.text:
						bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
					else:  # von bis
						bishops = get_information_v2(configeration, bishops, url, index, "von", "bis", "Name")
			# if it's not under a seciotn, and it's under the main titlee, then 
			# search for anything with a table 
			# get only THE FIRST table 
			section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&format=json")
			if "Amtszeit" in section_response.text:
				bishops = get_information_v1(0, bishops, url, "")
			elif "Amtsantritt" in section_response.text:
				bishops = get_information_v2(0, bishops, url, "",  "Amtsantritt", "Amtsende", "Name")
			else:  # von bis
				bishops = get_information_v2(0, bishops, url, "", "von", "bis", "Name")
		except:
			failed.append(url)
			continue

	bishops.to_csv("draft_bishops.csv")

def clean_up_rows(): 
	# clean up from/to
	# if "from has two 4 digit numbers then extrac tht eone that is  less 
	# if "to" has two 4 digit numer shtan put donw the one that is the most 
	bishop_df = pd.read_csv("draft_bishops_with_bios.csv")
	new_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	for i in range(len(bishop_df)):
		row = bishop_df.iloc[i]
		from_years = re.findall('(?<!\d)\d{4}|\d{3}(?!\d)', row["from"])
		if len(from_years) > 0:
			min_year = from_years[0]
		else:
			min_year = row["from"]
		if len(from_years) > 1:
			from_years = np.array(from_years).astype(int)
			min_year = np.min(from_years)

		to_years = re.findall('(?<!\d)\d{4}|\d{3}(?!\d)', row["to"])
		if len(to_years) > 0:
			max_year = to_years[0]
		else:
			max_year = row["to"]
		if len(to_years) > 1:
			to_years = np.array(to_years).astype(int)
			max_year = np.max(to_years)
		if str(min_year).isdigit():
			if int(min_year) < 1700:
				new_df.loc[len(new_df)] = [row["name"], min_year, max_year, row["biography"], row["biography_eng"], row["url"], row["bishop_type"], row["orig_url_page"]]
	new_df.to_csv("draft_bishops_final_with_bios.csv")

def get_biography(): 
	"""
	Step 2: run this after intiial_bishop_Srape() function. 
	This goes through and checks for any redirect links, and then gets the biography from those redirect links. 
	"""
	bishop_df = pd.read_csv("draft_bishops_final_with_bios.csv")
	new_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	# replace with ""
	bishop_df["url"] = bishop_df['url'].fillna("")
	bishop_df["url"] = bishop_df["url"].apply(lambda x: x.replace("/\"/", "/"))
	no_cell_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	# only get bishops that are before 1700. 
	for i in range(701, len(bishop_df)):
		row = bishop_df.iloc[i]
		text = ""
		curr = 0
		google_translate = ""
		new_df.loc[len(new_df)] = row
		if len(row["url"]) > 0 and "wiki/" in row["url"] and len(str(row["biography"])) < 4:
			dirlink = "https://de.wikipedia.org/w/api.php?action=parse&page="
			key = row["url"].split("wiki/")[1]
			if "jpg" in key:
				key = key.split(".jpg")[0]
				if "Datei" in key:
					key = key.split("Datei:")[1]
			biography = get(dirlink + key + "&section="+str(curr) )
			if "redirectText" in biography.text:
				biography = get(dirlink + key + "&section="+str(curr)+"&format=json" ).json()
				html_text = BeautifulSoup(biography["parse"]["text"]["*"])
				key = str(html_text.find_all("a")[0]).split("wiki/")[1].split("\"")[0]
				if "jpg" in key:
					key = key.split(".jpg")[0].split("Datei:")[1]
			biography = get(dirlink + key+"&section="+str(curr)+"&format=json" ).json()
			# if the biography is still nothing 
			if "error" in biography:
				#. then search it online. 
				key = key.replace("_", "%20")
				new_biography = get("https://de.wikipedia.org/w/api.php?action=query&list=search&srsearch=" + key+"&format=json" ).json()
				if len(new_biography["query"]["search"]) > 0:
					temp_key = new_biography["query"]["search"][0]["pageid"]
					temp_dirlink = "https://de.wikipedia.org/w/api.php?action=query&prop=info&pageids="
					biography = get(temp_dirlink+ str(temp_key)+"&&inprop=url&format=json"  ).json()
					key = biography["query"]["pages"][str(temp_key)]["title"].replace(" ", "_")
					biography = get(dirlink + key+"&section="+str(curr)+"&format=json" ).json()

			while len(text.split(" ")) < 400 and "error" not in biography:
				curr += 1
				html_text = BeautifulSoup(biography["parse"]["text"]["*"])
				bio_text = html_text.find_all("p")
				if len(bio_text) == 0:
					bio_text = html_text.find_all("li")
				cleanr = re.compile('<.*?>')
				bio = [str(s) for s in bio_text]
				bio_text_clean = re.sub(cleanr, '', "".join(bio))
				# get only 400 words. 
				num_words = bio_text_clean.split(" ")
				if len(num_words) > 200:
					bio_text_clean = " ".join(num_words[:200])
				text += bio_text_clean
				time.sleep(10)
				translation = translate_client.translate(text,target_language="en")
				google_translate = translation['translatedText']
				biography = get(dirlink + key+"&section="+str(curr)+"&format=json" ).json()


			new_df.loc[len(new_df)] = [row["name"], row["from"], row["to"], text, google_translate.encode('utf-8'), row["url"], row["bishop_type"], row["orig_url_page"]]
		new_df.to_csv("draft_bishops_final_with_bios_final_2.csv")
	import pdb; pdb.set_trace()
# and then we can just do the new_df. 

get_biography()
