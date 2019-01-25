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
import pdb

def get_information_v1(configeration, bishops, orig_url, index, word_reign=""):
	if index == "":
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&format=json").json()
	else:
		section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+orig_url+"&section="+index+"&format=json").json()
	html_text = BeautifulSoup( section_response["parse"]["text"]["*"])
	columns = html_text.find_all("th")
	if len(columns) == 0:
		columns = html_text.find_all("b")
	if len(columns) == 0: # then, we have a case of no bold text
		columns = html_text.find_all("tr")[0] # get the first row
		columns = columns.find_all("td")
	for i in range(len(columns)):
		cleanr = re.compile('<.*?>')
		column = re.sub(cleanr, '', str(columns[i])).replace("\n", "")
		if str(column).lower() == word_reign.lower():
			word_reign_index = i
		if str(column).lower() == "name" or "bischof" in str(columns[i]).lower():
			name_index = i
	num_columns = len(columns)
	rows = html_text.find_all("tr")
	for row in rows:
		try:
			cells = row.find_all("td")
			if len(cells) < num_columns:
				continue
			cleanr = re.compile('<.*?>')
			name = re.sub(cleanr, '', str(cells[name_index]))
			if "</a" in name:
				name = name.replace("</a", "")
			url =re.search(r'href=[\'"]?([^\'" >]+)', str(cells[name_index])).group(0).replace("href=", "")
			if url is not None:
				url = "https://de.wikipedia.org/"+ url
			from_to = str(cells[word_reign_index]).replace("<td>", "").replace("</td>", "").split('\xe2\x80\x93')
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
	columns = [col for col in columns if "align" not in str(col)] # this is for those that don't have
	if len(columns) == 0:
		table = html_text.find_all("table")
		columns =  BeautifulSoup(str(table)).find_all("b")
	num_columns = len(columns)
	from_index = 1
	to_index = 2
	name_index = 0
	for i in range(len(columns)):
		if str(columns[i]).lower() == from_key:
			from_index = i
		if  str(columns[i]).lower() == to_key :
			to_index = i
		if name_key in str(columns[i]) or "Bischof" in str(columns[i]):
			name_index = i
	rows = html_text.find_all("tr")
	for row in rows:
		try:
			url = ""
			cells = row.find_all("td")
			if len(cells) < num_columns or "<b>" in str(row):
				continue
			cleanr = re.compile('<.*?>')
			url_match = re.findall(r'href=[\'"]?([^\'" >]+)', str(cells[name_index]))
			if ("Hl" in str(cells[name_index]) or "Sel." in str(cells[name_index])) and len(url_match) > 1:
				url = "https://de.wikipedia.org/" +url_match[-1] # we assume the last one has the actual URL
			if len(url_match) == 1:
				url = url_match[0]
				url = "https://de.wikipedia.org/" +url
			name = re.sub(cleanr, '', str(cells[name_index]))
			from_val= str(cells[from_index])
			from_val = re.sub(cleanr, '', str(from_val))
			to_val = str(cells[to_index]).replace("<td>", "")
			to_val = re.sub(cleanr, '', str(to_val))
			bishops.loc[len(bishops)] = [name, from_val, to_val, "", url, str(configeration), orig_url]
		except:
			pass
	return bishops

def get_text(html, orig_url, bishops):
	list_bishops = html.find_all("li")
	for b in list_bishops:
		b = str(b)
		cleanr = re.compile('<.*?>')
		cleaned_b = re.sub(cleanr, '', b)
		name = cleaned_b
		if "(" in cleaned_b:
			name = cleaned_b.split("(")[0]
		vals = cleaned_b.split("\xe2\x80\x93")
		to_val = ""
		if len(vals) == 2:
			to_val = vals[1]
		from_val =  re.sub("[^0-9]", "", vals[0])
		if "erwähnt" in cleaned_b:
			if "und" in cleaned_b:
				from_val = cleaned_b.split("und")[0].split("erwähnt")[1]
				to_val = cleaned_b.split("und")[1].split(")")[0]
			else:
				from_val = cleaned_b.split("erwähnt")[1].split(")")[0]
		url = ""
		try:
			url = b.split("href=")[1].split("\"")[1]
			url = "https://de.wikipedia.org/"+url
		except:
			pass
		configeration = 0
		bishops.loc[len(bishops)] = [name, from_val, to_val, "", url, str(configeration), orig_url]
	return bishops

def initial_bishop_scrape(urls=None):
	"""
	Initial bishop scrape, this will get the names, year in, yea rout (but iwthout clenaing), and the
	links to each bishops' wikipedia page.


	"""

	failed = []
	bishops = pd.DataFrame(columns=["name", "from" , "to", "biography", "url", "bishop_type","orig_url_page"])
	for url in urls:
		print(url)
		import pdb; pdb.set_trace()
		got_bishops = False
		prev_length = len(bishops)
		response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&prop=sections&format=json").json()
		try:
			sections = response["parse"]["sections"]
			for section in sections:
				if section["line"] == u'Bischöfe' \
					or  section["line"] == u'Bisch\xc3\xb6fe'\
					or " Bisch" in  section["line"].encode('utf-8')\
					or "Bisch\xc3\xb6fe" in section["line"].encode('utf-8')\
					or "gr\xfcndung" in section["line"].encode('utf-8'):
					index = str(section["index"])
					configeration = 0
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index+"&format=json").json()
					html_text = BeautifulSoup(section_response["parse"]["text"]["*"])
					columns = html_text.find_all("th")
					if len(columns) == 0:
						table = html_text.find_all("table")
						columns =  BeautifulSoup(str(table)).find_all("b")
					# if columns is still 0, this mean we are dealing with text (bulleted) data.
					for col in columns:
						col = str(col)
						if "Amtszeit" in col:
							bishops = get_information_v1(configeration, bishops, url, index,"Amtszeit" )
						elif "Regierungszeit" in col:
							bishops = get_information_v1(configeration, bishops, url, index, "Regierungszeit")
						elif  "Amtsantritt" in col:
							bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
						elif "bis" in col:
							bishops = get_information_v2(configeration, bishops, url, index, "von", "bis" ,"Name")

				elif section["line"] == u'F\u00fcrstbisch\u00f6fe':
					index = str(section["index"])
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index)
					configeration = 2
					if "Amtszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index,"Amtszeit" )
					elif "Regierungszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index, "Regierungszeit")
					elif "Amtsantritt" in section_response.text:
						bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
					else:  # von bis
						bishops = get_information_v2(configeration, bishops, url, index, "von", "bis", "Name")
				elif "Erzbisch\xf6fe" in  section["line"].encode('utf-8')\
				 	or "Erzbisch\xc3\xb6fe" in section["line"].encode('utf-8')\
					or "Erzbischöfe" in section["line"].encode("utf-8"):
					index = str(section["index"])
					section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&section="+index)
					configeration = 1
					pdb.set_trace()
					if "Amtszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index,"Amtszeit" )
					elif "Regierungszeit" in section_response.text:
						bishops = get_information_v1(configeration, bishops, url, index, "Regierungszeit")
					elif "Amtsantritt" in section_response.text:
						bishops = get_information_v2(configeration, bishops, url, index, "Amtsantritt", "Amtsende", "Name")
					else:  # von bis
						bishops = get_information_v2(configeration, bishops, url, index, "von", "bis", "Name")
				if len(bishops) > prev_length:
						got_bishops = True
			if got_bishops is False:
				section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&format=json").json()
				html_text = BeautifulSoup(section_response["parse"]["text"]["*"])
				import pdb; pdb.set_trace()
				if "li" in str(html_text):
					pdb.set_trace()
					bishops = get_text(html_text, url, bishops)
					continue
				columns = html_text.find_all("th")
				if len(columns) > 0:
					configeration = 0

					if "Amtszeit" in str(html_text):
						bishops = get_information_v1(configeration, bishops, url, str(0),"Amtszeit" )
					elif "Regierungszeit" in str(html_text):
						bishops = get_information_v1(configeration, bishops, url, str(0), "Regierungszeit")
					elif "Amtsantritt" in str(html_text):
						bishops = get_information_v2(configeration, bishops, url, str(0), "Amtsantritt", "Amtsende", "Name")
					else:  # von bis
						bishops = get_information_v2(configeration, bishops, url, str(0), "von", "bis", "Name")
				else:
					bishops = get_text(html_text, url, bishops)

			# if it's not under a section and it's under the first title
			section_response = get("https://de.wikipedia.org/w/api.php?action=parse&page="+url+"&format=json").json()
			if "Amtszeit" in section_response["parse"]["text"]:
				bishops = get_information_v1(0, bishops, url, "")
			elif "Amtsantritt" in section_response["parse"]["text"]:
				bishops = get_information_v2(0, bishops, url, "",  "Amtsantritt", "Amtsende", "Name")
			else:  # von bis
				bishops = get_information_v2(0, bishops, url, "", "von", "bis", "Name")
		except Exception as e:
			print(e.message)
			failed.append(url)
			continue

	import pdb; pdb.set_trace()
	bishops.to_csv("draft_bishops_try.csv")

def clean_up_rows():
	# this cleans up any
	# clean up from/to
	# this also cleans up names in case there is anything unexpected in the names.
	# if "from" has two 4 digit numbers then extrac the one that is less
	# if "to" has two 4 digit numer shtan put down the one that is the most
	bishop_df = pd.read_csv("draft_bishops_try.csv")
	new_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	for i in range(len(bishop_df)):
		row = bishop_df.iloc[i]
		if row["name"] == "\n" or isinstance(row["name"], float) or "vakant" in row["name"]:
			continue
		from_years = re.findall('(?<!\d)\d{4}|\d{3}(?!\d)', str(row["from"]))
		whitelist = set('abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ')
		name = ''.join(filter(whitelist.__contains__, row["name"]))
		if "um" in name:
			name = name.replace("um", "")
		if "nach" in name:
			name = name.replace("nach", "")
		if len(from_years) > 0:
			min_year = from_years[0]
		else:
			min_year = row["from"]
		if len(from_years) > 1:
			from_years = np.array(from_years).astype(int)
			min_year = np.min(from_years)

		to_years = re.findall('(?<!\d)\d{4}|\d{3}(?!\d)', str(row["to"]))
		if len(to_years) > 0:
			max_year = to_years[0]
		else:
			max_year = row["to"]
		if len(to_years) > 1:
			to_years = np.array(to_years).astype(int)
			max_year = np.max(to_years)
		if str(min_year).isdigit():
			if int(min_year) < 1700:
				new_df.loc[len(new_df)] = [name, min_year, max_year, row["biography"], "", row["url"], row["bishop_type"], row["orig_url_page"]]
	new_df.to_csv("draft_bishops_try_cleaned.csv")

def get_biography():
	"""
	Step 2: run this after intiial_bishop_Srape() function.
	This goes through and checks for any redirect links, and then gets the biography from those redirect links.
	"""
	bishop_df = pd.read_csv("draft_bishops_try_cleaned.csv")
	new_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	# replace with ""
	bishop_df["url"] = bishop_df['url'].fillna("")
	bishop_df["url"] = bishop_df["url"].apply(lambda x: x.replace("/\"/", "/"))
	no_cell_df = pd.DataFrame(columns=["name", "from" , "to", "biography", "biography_eng",  "url", "bishop_type","orig_url_page"])
	# only get bishops that are before 1700.
	for i in range(27, len(bishop_df)):
		row = bishop_df.iloc[i]
		text = ""
		curr = 0
		google_translate = ""
		new_df.loc[len(new_df)] = row
		if len(row["url"]) > 0 and "wiki/" in row["url"] and len(str(row["biography"])) < 4 and row["url"].split("wiki/")[1].isdigit() is False:
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
			new_df.loc[len(new_df) - 1] = [row["name"], row["from"], row["to"], text, google_translate.encode('utf-8'), row["url"], row["bishop_type"], row["orig_url_page"]]
		new_df.to_csv("draft_bishops_final_with_bios_Poland.csv")

template = "Liste_der_Erzbischöfe_von_"
import pandas as pd

csv = pd.read_csv("AdditionalDioceses.csv")
spain_only = csv['Belgium'].dropna(axis=0).tolist()
spain = map(lambda x: template+x, spain_only)
last_spain = pd.read_csv("final_bishops_Spain.csv")
current_dioces = set(last_spain["orig_url_page"])
spain_left = []
spain_left.append("Liste_der_Erzbischöfe_von_Gniezno")
spain_left.append("Liste_der_Bischöfe_von_Koszalin-Kołobrzeg")
spain_left.append("Liste_der_Bischöfe_von_Lebus")
spain_left.append("Bistum_Pelplin")
spain_left.append("Liste_der_Bischöfe_von_Płock")
spain_left.append("Liste_der_Bischöfe_und_Erzbischöfe_von_Posen")
spain_left.append("Liste_der_Bischöfe_von_Veszprém")
spain_left.append("Liste_der_Bischöfe_von_Włocławek")
initial_bishop_scrape(spain_left)
