import requests,json,time,sys,re,os,sys,csv,re,traceback,MySQLdb
import urllib.request
from selenium import webdriver
from datetime import date,datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

class Scrape(object):
	def __init__(self):
		self.mydict={}
		self.count=1
		self.is_header = True
		self.BLUE = '\033[94m'
		self.GREEN = '\033[92m'
		self.RED   = "\033[1;31m"
		self.RESET = "\x1b[0m"
		self.BG = "\x1b[6;30;42m"
		self.BOLD    = "\033[;1m"
		self.WARNING = '\033[93m'
		self.BG_underline = "\x1b[9;22;28m"
		self.BG_RED = "\x1b[7;10;20m"
		self.is_updated = False
		dt = "{:%B %d, %Y %H-%M-%S}".format(datetime.now())
		print("="*50+"START PROCESSING ("+str(dt)+") "+"="*50)
		self.path_var_create_dir()
		self.log_file.write("="*50+"START PROCESSING ("+str(dt)+") "+"="*50+'\n')
		# self.db_connection()
		self.initialize_driver()


	def path_var_create_dir(self):
		self.chromedriver= 'D:\\desktop\\promotheus\\appartment\\chromedriver\\chromedriver.exe'
		
		# self.chromedriver= 'chromedriver.exe'  
		print(os.getcwd())
		self.csv_cityname_data = "D:\\desktop\\promotheus\\appartment\\city_name.csv"
		print(self.csv_cityname_data)
		self.urls_file_path = os.getcwd() + '/json_data/'
		ts = datetime.now().strftime("%Y%m%d%I%M%S")
		# self.csv_output = os.getcwd() + '/output/property_'+str(ts)+'/'
		self.logs = 'log/'
		self.csv_output = 'C:/inetpub/wwwroot/HearthExcelData/HearthExcelData/Out/'
		#C:\Users\adarsh.gupta\workspace\python\scripts\extraction\apartment
		if not os.path.exists(self.chromedriver):
			exit('Please check the chromedriver path !!')
		if not os.path.exists(self.csv_cityname_data):
			exit('Please check the location name csv file path !!')
		if not os.path.exists(self.urls_file_path):
			os.mkdir(self.urls_file_path)
		if not os.path.exists(self.csv_output):
			os.mkdir(self.csv_output)
		log_path = os.getcwd() + '/log/apartment_'
		if not os.path.exists(self.logs):
			os.mkdir(self.logs)

			ts = datetime.now().strftime("%Y%m%d%I%M%S")
			self.log_file=open(log_path+str(ts)+".log", "a+")
			# self.log_file=open("log/apartment_"+str(ts)+".log", "a+")
		else:
			ts = datetime.now().strftime("%Y%m%d%I%M%S")
			self.log_file=open(log_path+str(ts)+".log", "a+")
			# self.log_file=open("log/apartment_"+str(ts)+".log", "a+")
	
	def initialize_driver(self):

		try:
			self.options = webdriver.ChromeOptions()
			#self.options.add_argument('headless')
			self.options.add_argument('--start-maximized')
			self.options.add_argument("--disable-infobars")
			self.options.add_argument("--disable-extensions")
			self.options.add_argument("--disable-notifications")
			self.options.add_argument("--disable-popup-blocking")
			os.environ["webdriver.chrome.driver"] = self.chromedriver
			# self.driver=webdriver.Chrome(ChromeDriverManager().install(), options=self.options)
			self.driver = webdriver.Chrome(executable_path=self.chromedriver, options=self.options)
			print(self.BLUE+"Chrome Browser Launched !!!"+self.RESET)
			self.log_file.write('Chrome Browser Launched !!!...\n')
		except Exception as e:
			exit(self.RED+str(e))

	def db_connection(self):
		
		try:
			self.db = MySQLdb.connect(host="localhost", port=3306,user="webscrap", passwd="webscrap123", db='property', charset='utf8mb4',use_unicode=True)
			# self.db = MySQLdb.connect(host="localhost", port=3306,user="root", passwd="Welcome@123", db='appart', charset='utf8',use_unicode=True)
			# self.db = MySQLdb.connect(host="localhost", port=3306,user="root", password="Welcome@321", db='appart', charset='utf8',use_unicode=True)
			self.cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
			print(self.GREEN+'Database connection successful...'+self.RESET)
			self.log_file.write('Database connection successful...\n')
		except Exception as e:
			dt = "{:%B %d, %Y %H-%M-%S}".format(datetime.now())
			self.log_file.write(str(e)+'\n'+"="*50+"PROCESS END ("+str(dt)+") "+"="*50+'\n')
			# exit(self.RED+'Exception --->'+str(e)+self.RESET+'\n'+"="*50+"PROCESS END ("+str(dt)+") "+"="*50)


	def read_city_fromCSV(self):
		self.all_city_name=[]
		with open(self.csv_cityname_data, 'r') as csvfile:
			reader = csv.reader(csvfile)
			for row in reader:
				self.all_city_name.append("".join(row))
		if ((len(self.all_city_name)) == 0):
			self.log_file.write('No data in CSV file...\n')
			exit(self.RED+'No data in CSV file'+self.RESET)
		

	def Is_all_urls_list(self,question, default="yes"):
		valid = {"yes": True, "y": True,"no": False, "n": False,"quit":"Quit"}
		if default is None:
			prompt = " [y/n] "
		elif default == "yes":
			prompt = " [Y/n] "
		elif default == "no":
			prompt = " [y/N] "
		else:
			raise ValueError("invalid default answer: '%s'" % default)
		while True:
			sys.stdout.write(question + prompt)
			choice = input().lower()
			if default is not None and choice == '':
				return valid[default]
			elif choice in valid:
				return valid[choice]
			else:
				sys.stdout.write("Please respond with 'yes' or 'no' "
									"(or 'y' or 'n').\n")

	def initialize_variable(self):
		arg_names = ['command', 'city_name']
		args = dict(zip(arg_names, sys.argv))
		self.mydict['city_name'] = args.get("city_name")
		if self.mydict['city_name'] ==None:
			exit(self.RED+'please enter command with valid parameters ie.'+self.RESET+self.WARNING+' python '+str(args['command'])+' "city name"'+self.RESET)
		print(self.GREEN+'Initialized successfully...'+self.RESET)


	def input_city_name(self,question):
		while True:
			sys.stdout.write(self.WARNING+question+self.RESET)
			choice = input().lower()
			if choice == '':
				exit(self.RED+'Please enter city name'+self.RESET)
			else:
				return choice

	def getURL(self):
		self.driver.get('https://www.apartments.com/')
		time.sleep(5)
		print(self.driver)
		print("****************")
		inputsearchbox = self.driver.find_element(By.XPATH, "//*[@id='quickSearchLookup']")
		# inputsearchbox.send_keys(str(self.mydict['city_name']))
		inputsearchbox.send_keys(str(self.mydict['city_name'])+' CA')
		time.sleep(7)
		searchButton = self.driver.find_element(By.XPATH, "//*[@id='quickSearch']/div/div/a")
		searchButton.click()
		time.sleep(10)



	def find_number(self,text, c):
		pages_list = re.findall(r'%s(\d+)' % c, text)
		total = ''.join(pages_list)
		return total

	def get_total_pages(self):
		self.driver.execute_script("document.body.style_zoom='150%';")
		time.sleep(3)
		pages = self.driver.find_element(By.XPATH, "//*[@class='pageRange']")
		totalpages = self.find_number(pages.text, 'of ')
		return totalpages

	def create_all_url_list_file(self):
		jsonData = {}
		jsonData['list'] = self.alldataUrls
		with open(self.url_list_path, 'w') as f:
			json.dump(jsonData, f)
		print(self.GREEN+self.mydict['city_name']+' city data successfully instered in json file..'+self.RESET)

	def listingURL(self):
		self.alldataUrls=[]
		total_pages = self.get_total_pages()
		print(self.WARNING+'Total Pages --> '+total_pages+self.RESET)
		processor_url = self.driver.current_url
		offset = (100/int(total_pages))
		counter = offset
		url_counter=0
		page_counter=1
		#for i in range(1,int(total_pages)+1):
		for i in range(1):
			self.driver.get(processor_url+str(i))
			time.sleep(10)
			links = self.driver.find_elements(By.XPATH, "//div[@id='placardContainer']/ul/li/article")
			url_counter = url_counter+int(len(links))
			#links = self.driver.find_elements(By.XPATH, "//header[@class='placardHeader]/a")
			dataUrls = [link.get_attribute('data-url') for link in links]
			dataUrls = [x for x in dataUrls if x]
			self.alldataUrls = self.alldataUrls+dataUrls
			time.sleep(0.1)
			if counter < 100: 
				print('page( '+str(page_counter)+' / '+str(total_pages)+' )'+self.RED+'  [URLS -> '+str(url_counter)+'] please wait.... [%s%%]\r'%round(counter,2), end=""+self.RESET)
			else:
				counter = 100
				print('page( '+str(page_counter)+' / '+str(total_pages)+' )'+self.RED+'  [URLS -> '+str(url_counter)+'] please wait.... [%s%%]\r'%round(counter,2), end=""+self.RESET)
			counter = counter+offset
			page_counter = page_counter+1
		self.create_all_url_list_file()
		



	def inser_appartment_data(self):
		try:
			if self.is_exist_record():
				print(self.WARNING+'This property alread Exists !!'+self.RESET)
				#sql = "UPDATE `appartment` SET `property_name`='%s',`property_address`='%s',`city`='%s', `property_modified`=CURRENT_TIMESTAMP WHERE `property_url`='%s' and id = %s"%(self.appartment_dict['propertyName'],self.appartment_dict['addressEle'],self.mydict['city_name'],self.appartment_dict['url'],int(self.updated_id))
				sql = "UPDATE `appartment` SET `property_name`='%s',`property_address`='%s',`city`='%s',`rent_special`='%s', `property_modified`=CURRENT_TIMESTAMP WHERE `property_url`='%s' and id = %s"%(str(self.appartment_dict['propertyName']),str(self.appartment_dict['addressEle']),str(self.mydict['city_name']),str(self.appartment_dict['rent_special']),str(self.appartment_dict['url']),int(self.updated_id))                
				self.cursor.execute(sql)
				self.db.commit()
				#print(self.GREEN+'property successfully update'+self.RESET)
				return self.updated_id
			else:
				sql = "INSERT INTO `appartment`(`property_name`, `property_address` ,`city`,`rent_special`, `property_url`, `property_created`, `property_modified`) VALUES ('%s','%s','%s','%s','%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(str(self.appartment_dict['propertyName']),str(self.appartment_dict['addressEle']),str(self.mydict['city_name']),str(self.appartment_dict['rent_special']),str(self.appartment_dict['url']))
				self.cursor.execute(sql)
				self.db.commit()
				#print(self.GREEN+'Appartment data successfully insert !!'+self.RESET)
				return self.cursor.lastrowid
		except:
			traceback.print_exc()


	def insert_property_service(self):
		if 'pet_policy' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['pet_policy']),int(self.Apartment_Amenities['appartment_id']),int(2))
					#print(self.WARNING+'property pet policies successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(2),json.dumps(self.Apartment_Amenities['pet_policy']))
					#print(self.GREEN+'property pet policies successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()

		if 'lease_length' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['lease_length']),int(self.Apartment_Amenities['appartment_id']),int(1))
					#print(self.WARNING+'property lease policies successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(1),json.dumps(self.Apartment_Amenities['lease_length']))
					#print(self.GREEN+'property lease policies successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()

		if 'services' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['services']),int(self.Apartment_Amenities['appartment_id']),int(3))
					#print(self.WARNING+'property other services successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(3),json.dumps(self.Apartment_Amenities['services']))
					#print(self.GREEN+'property other services successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()


		if 'sound_score' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['services']),int(self.Apartment_Amenities['appartment_id']),int(4))
					#print(self.WARNING+'property sound score successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(4),json.dumps(self.Apartment_Amenities['sound_score']))
					#print(self.GREEN+'property sound score successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()



		if 'walk_score' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['services']),int(self.Apartment_Amenities['appartment_id']),int(5))
					#print(self.WARNING+'property walk score successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(5),json.dumps(self.Apartment_Amenities['walk_score']))
					#print(self.GREEN+'property walk score successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()


		if 'school_data' in self.Apartment_Amenities.keys():
			try:
				if self.is_updated:
					sql = "UPDATE `property_services` SET `service_value`='%s',`service_modified`=CURRENT_TIMESTAMP WHERE appartment_id = %s and services_type = %s"%(json.dumps(self.Apartment_Amenities['services']),int(self.Apartment_Amenities['appartment_id']),int(6))
					#print(self.WARNING+'property school data successfully updated !!'+self.RESET)
				else:
					sql = "INSERT INTO `property_services`(`appartment_id`, `services_type` , `service_value`, `services_created`, `service_modified`) VALUES (%s,%s,'%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(self.Apartment_Amenities['appartment_id']),int(6),json.dumps(self.Apartment_Amenities['school_data']))
					#print(self.GREEN+'property school data successfully insert !!'+self.RESET)
				self.cursor.execute(sql)
				self.db.commit()
			except:
				traceback.print_exc()



	def get_walk_score(self):
		try:    
			walkscore=''
			score=self.driver.find_element(By.XPATH, "//*[@class='ratingCol walkScore']")
			walkscore= score.text
			return walkscore
		except:
			traceback.print_exc()
			return walkscore


	def get_schools_data(self):
		datalist=[]
		elements_data = self.driver.find_elements(By.XPATH, '//*[@class="cell-sm-6 cell-xs-12 paddingReset"]')
		for data in elements_data:
			school_dict={}
			school_dict['school_name'] = data.find_element(By.XPATH, '//p[@class="schoolName"]/a').text
			school_dict['school_type'] = data.find_element(By.XPATH, '//p[@class="schoolType"]').text
			school_dict['school_desc'] = data.find_element(By.XPATH, '//div[@class="details"]').text
			try:
				school_rating = data.find_element(By.XPATH, '//div[@class="schoolRating "]/i')
				san = [int(s) for s in school_rating.get_attribute('class') if s.isdigit()]
				school_dict['school_rating'] = san[0]
			except:
				school_dict['school_rating'] = ''
				pass
			datalist.append({k: v for k, v in school_dict.items() if v !=''})
		return datalist



	def get_services_data(self,current_data_id):
		element_data = self.driver.find_elements(By.XPATH, '//section[@class="specGroup js-specGroup shuffle"]/div')
		self.Apartment_Amenities = {}
		for data in element_data:
			recordData = data.find_element(By.XPATH, './h3')
			if recordData.text == "Pet Policy" :
				self.Apartment_Amenities["pet_policy"] = {}
				innerParaEle = data.find_elements(By.XPATH, './div[@class="petPolicyDetails"]')
				self.allInnerHeader = []
				for idx , paraEle in enumerate(innerParaEle):
					innerHeaderData = paraEle.find_elements(By.XPATH, './p')
					self.allInnerData = []
					for inData in innerHeaderData:
						san = inData.text
						san = re.sub('\n', '', san)
						san = re.sub('•', '', san)
						self.allInnerHeader.append(san)
					innerListData = paraEle.find_elements(By.XPATH, './ul/li')
					for listData in innerListData:
						san = listData.text
						san = re.sub('\n', '', san)
						san = re.sub('•', '', san)
						self.allInnerData.append(san)
					self.Apartment_Amenities["pet_policy"][self.allInnerHeader[idx]] = self.allInnerData

			if recordData.text == "Lease Length" :
				self.Apartment_Amenities["lease_length"] = {}
				allInnerData = []
				innerListData = data.find_elements(By.XPATH, './ul/li')
				for listData in innerListData:
					san = listData.text
					san = re.sub('\n', '', san)
					san = re.sub('•', '', san)
					allInnerData.append(san)
				self.Apartment_Amenities['lease_length'] = allInnerData

			if recordData.text == "Services" :
				self.Apartment_Amenities["services"] = {}
				allInnerData = []
				innerListData = data.find_elements(By.XPATH, './ul/li')
				for listData in innerListData:
					san = listData.text
					san = re.sub('\n', '', san)
					san = re.sub('•', '', san)
					allInnerData.append(san)
				self.Apartment_Amenities['services'] = allInnerData
		self.Apartment_Amenities['appartment_id'] = current_data_id
		self.Apartment_Amenities['sound_score'] = self.get_sound_score()
		self.Apartment_Amenities['walk_score'] = self.get_walk_score()
		self.Apartment_Amenities['school_data'] = self.get_schools_data()
		


		self.insert_property_service()


	def insert_property_details(self):
		for data in self.property_details:
			try:
				if self.is_updated:
					delete = "DELETE FROM `propery_details` WHERE appartment_id = %s"%(int(data['appartment_id']))
					self.cursor.execute(delete)
					self.db.commit()
					sql = "INSERT INTO `propery_details`(`appartment_id`, `propery_type` , `price`,`area`,`availability`,`details_created`,`details_modified`) VALUES (%s,'%s','%s','%s','%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(data['appartment_id']),data['bed']+'  '+data['broom'],data['price'],data['area'],data['availablity'])
					self.is_updated=False
				else:
					sql = "INSERT INTO `propery_details`(`appartment_id`, `propery_type` , `price`,`area`,`availability`,`details_created`,`details_modified`) VALUES (%s,'%s','%s','%s','%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(int(data['appartment_id']),data['bed']+'  '+data['broom'],data['price'],data['area'],data['availablity'])
				self.cursor.execute(sql)
				self.db.commit()
				#print(self.GREEN+'Appartment details successfully insert !!'+self.RESET)
			except:
				traceback.print_exc()



	def get_property_detail(self,url,current_data_id):
		try:
			self.property_details = []
			self.driver.get(url)
			time.sleep(5)
			allTable_data = self.driver.find_elements(By.XPATH, '(//div[@class="js-expandableContainer expandableContainer"])[1]/table/tbody/tr')
			#allTable_data = self.driver.find_elements(By.XPATH, '(//div[@class="js-expandableContainer expandableContainer"])[1]/table/tbody/tr[@class="rentalGridRow   bold first"]')
			for table_data in allTable_data:
				allTableDict = {}
				allTableDict['bed'] = table_data.find_element(By.XPATH, '//td[@class="beds"]').text
				allTableDict['broom'] = table_data.find_element(By.XPATH, '//*[@class="baths"]').text
				allTableDict['price'] = table_data.find_element(By.XPATH, '//td[@class="rent"]').text
				allTableDict['area'] = table_data.find_element(By.XPATH, '//td[@class="sqft"]').text
				allTableDict['availablity'] = table_data.find_element(By.XPATH, '//td[@class="available"]').text
				allTableDict['appartment_id'] = current_data_id
				self.property_details.append(allTableDict)
		except Exception as e:
			try:
				allTableDict = {}
				allTableDict['bed'] = self.driver.find_element(By.XPATH, '//*[@id="rentRollups"]//span/span[1]').text
				allTableDict['broom'] = self.driver.find_element(By.XPATH, '//*[@class="baths"]').text
				allTableDict['price'] = self.driver.find_element(By.XPATH, '//*[@class="rent"]').text
				allTableDict['area'] = self.driver.find_element(By.XPATH, '//*[@class="sqft"]').text
				allTableDict['availablity'] = table_data.find_element(By.XPATH, '//*[@class="available"]').text
				allTableDict['appartment_id'] = current_data_id
				self.property_details.append(allTableDict)
			except Exception as e:
				print('Exception -->',str(e))
				pass
		if self.property_details:
			self.insert_property_details()

	def get_appartment_data(self,url,city):
		self.appartment_dict={}
		
		try:
			self.appartment_dict['propertyName']=self.driver.find_element(By.XPATH, '//h1[@class="propertyName"]').text
		except Exception as e:
			print(self.RED+'Exception in property name extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property name extraction -->'+str(e)+'\n')
			self.appartment_dict['propertyName'] = 'Not Found'
		
		self.appartment_dict['propertyurl'] = url
		self.appartment_dict['propertycity'] = city
		
		try:
			self.appartment_dict['addressEle'] = self.driver.find_element(By.XPATH, '//div[@class="propertyAddressContainer"]/h2').text
		except Exception as e:
			print(self.RED+'Exception in property address extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property address extraction -->'+str(e)+'\n')
			self.appartment_dict['propertyName'] = 'Not Found'
		
		try:
			self.appartment_dict['rent_special'] = self.driver.find_element(By.XPATH, '//*[@id="priceBedBathAreaInfoWrapper"]//li[1]//p[2]').text
		except Exception as e:
			print(self.RED+'Exception in property rent extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property rent extraction -->'+str(e)+'\n')
			self.appartment_dict['rent_special'] = 'Not Found'

		try:
			self.appartment_dict['bedroom'] = self.driver.find_element(By.XPATH, '//*[@id="priceBedBathAreaInfoWrapper"]//li[2]//p[2]').text
		except Exception as e:
			print(self.RED+'Exception in property bedroom extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in bedroom extraction -->'+str(e)+'\n')
			self.appartment_dict['bedroom'] = 'Not Found'

		try:
			self.appartment_dict['bathroom'] = self.driver.find_element(By.XPATH, '//*[@id="priceBedBathAreaInfoWrapper"]//li[3]//p[2]').text
		except Exception as e:
			print(self.RED+'Exception in property bathroom extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property bathroom extraction -->'+str(e)+'\n')
			self.appartment_dict['bathroom'] = 'Not Found'

		try:
			self.appartment_dict['area'] = self.driver.find_element(By.XPATH, '//*[@id="priceBedBathAreaInfoWrapper"]//li[4]//p[2]').text
		except Exception as e:
			print(self.RED+'Exception in property area extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property area extraction -->'+str(e)+'\n')
			self.appartment_dict['area'] = 'Not Found'

		try:
			self.appartment_dict['move_in_special'] = self.driver.find_element(By.XPATH, '//section[@id="rentSpecialsSection"]//p').text
		except Exception as e:
			print(self.RED+'Exception in property move_in_special extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in property move in special extraction -->'+str(e)+'\n')
			self.appartment_dict['move_in_special'] = 'Not Found'
		# current_data_id = self.inser_appartment_data()
		# return current_data_id



	def get_floorwise_plan(self):
		self.floorwise_datalist = []
		cnt = 1
		try:
			element_data = self.driver.find_elements(By.XPATH, '//*[@class="pricingGridItem multiFamily hasUnitGrid"]')
		except Exception as e:
				print(self.RED+'Exception in  main floor XPATH extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in main floor extraction  and system goes to exit-->'+str(e)+'\n')
				exit('system exit !!!!')

		if len(element_data) ==0:
			print("** Unit Not Available **")
			element_data = self.driver.find_elements(By.XPATH, '//*[@class="pricingGridItem multiFamily "]')
		for ele in element_data:
			floorwise_datadict={}
			try:
				floorwise_datadict['floor']= ele.find_element(By.XPATH, '//h3/span[1]').text
			except Exception as e:
				print(self.RED+'Exception in property floor name extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in property floor name extraction -->'+str(e)+'\n')
				floorwise_datadict['floor']='Not Found'
			
			try:
				floorwise_datadict['price_range']= ele.find_element(By.XPATH, '//h3/span[2]').text
			except Exception as e:
				print(self.RED+'Exception in property floor price range extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in property floor price range extraction -->'+str(e)+'\n')
				floorwise_datadict['price_range']= 'Not Found'

			try:
				floorwise_datadict['apt_type']= ele.find_element(By.XPATH, '//h4/span[1]').text
			except Exception as e:
				print(self.RED+'Exception in property apartment type extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in property apartment type extraction -->'+str(e)+'\n')
				floorwise_datadict['apt_type']= 'Not Found'

			try:
				floorwise_datadict['total_unit']= ele.find_element(By.XPATH, '//div[@class="availability"]').text
			except Exception as e:
				print(self.RED+'Exception in property totla unit extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in property total unit extraction -->'+str(e)+'\n')
				floorwise_datadict['total_unit']='Not Found'

			try:
				time.sleep(2)
				ele.find_element(By.XPATH, '//p//*[@class="js-priceGridShowMoreLabel"]').click()
			except Exception as e:
				print(self.RED+'Exception while click or show more items -->'+str(e)+self.RESET)
				self.log_file.write('Exception in click show more button extraction -->'+str(e)+'\n')
			try:
				floorwise_datadict['unit'] = self.get_unit(ele,cnt)
			except Exception as e:
				print(self.RED+'Exception in property unit data extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  roperty unit data extraction !! -->'+str(e)+'\n')

			cnt=cnt+1
			self.floorwise_datalist.append(floorwise_datadict)



	def get_unit(self,ele,cnt):
		XPATH = '//*[@id="availabilitySection"]/div[2]/div[%s]/div[2]//ul/li'%(cnt)
		try:
			data = ele.find_elements(By.XPATH, XPATH)
		except Exception as e:
				print(self.RED+'Exception in  get unit main XPATH extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  get unit main XPATH extraction system exiting !! -->'+str(e)+'\n')
				exit('System exit in unit main XPATH')

		unitlist = []
		#print('********Getting UNIT*******',len(data))
		for i in data:
			unitdict={}
			try:
				unit_no = i.find_element(By.XPATH, '//*[@class="unitColumn column"]').text
				unitdict['unit_no'] = "".join(unit_no.split('\n'))
			except Exception as e:
				print(self.RED+'Exception in unit no  extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  unit number extraction !! -->'+str(e)+'\n')
				unitdict['unit_no']='Not Found'

			try:
				unitprice = i.find_element(By.XPATH, '//*[@class="pricingColumn column"]').text
				unitdict['unitprice'] = "".join(unitprice.split('\n'))
			except Exception as e:
				print(self.RED+'Exception in  unit price extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  unit price extraction !! -->'+str(e)+'\n')
				unitdict['unitprice'] = 'Not Found'

			try:
				unitarea = i.find_element(By.XPATH, '//*[@class="sqftColumn column"]').text
				unitdict['unitarea'] = "".join(unitarea.split())
			except Exception as e:
				print(self.RED+'Exception in unit area  extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  unit area extraction !! -->'+str(e)+'\n')
				unitdict['unitarea']= 'Not Found'

			try:
				unit_availablity= i.find_element(By.XPATH, '//*[@class="availableColumn column"]').text
				unitdict['unitavailable']= "".join(unit_availablity.split('\n'))
			except Exception as e:
				print(self.RED+'Exception in check unit availability extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  unit availablity extraction !! -->'+str(e)+'\n')
				unitdict['unitavailable']='Not Found'

			#print(unitdict)
			unitlist.append(unitdict)
		return unitlist



	def get_amenities(self):
		self.aminities_dict={}
		try:
			self.aminities_dict['unique_feature'] = self.driver.find_element(By.XPATH, '//*[@id="99"]/ul').text
		except Exception as e:
			print(self.RED+'Exception in unique feature  extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in unique feature  extraction  -->'+str(e)+'\n')
			self.aminities_dict['unique_feature'] ='Not Found'

		try:
			#aminities_dict['common_feature'] = self.driver.find_element(By.XPATH, '//*[@id="amenitiesSection"]//div[@class="spec"]').text
			self.aminities_dict['common_feature'] = self.driver.find_element(By.XPATH, '//*[@id="amenitiesSection"]').text
		except Exception as e:
			print(self.RED+'Exception in common feature extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  common feature  extraction  -->'+str(e)+'\n')
			self.aminities_dict['common_feature'] ='Not Found'



	def get_contact_details(self):
		self.contact_dict={}
		try:
			self.contact_dict['mobile'] = self.driver.find_element(By.XPATH, '//*[@id="officeHoursSection"]//*[@class="phoneNumber"]//span').text
		except Exception as e:
			print(self.RED+'Exception in  contace mobile no extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  contact mobile number  extraction  -->'+str(e)+'\n')
			self.contact_dict['mobile']="Not Found"

		try:
			self.contact_dict['website'] = self.driver.find_element(By.XPATH, '//*[@id="officeHoursSection"]//*[@class="mortar-wrapper"]/a').get_attribute('href')
		except Exception as e:
			print(self.RED+'Exception in contace email extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  email extraction  -->'+str(e)+'\n')
			self.contact_dict['website'] = 'Not Found'

		try:
			self.contact_dict['timing'] = self.driver.find_element(By.XPATH, '//*[@id="officeHoursSection"]//*[@class="todaysHours clearfix"]//span').text
		except Exception as e:
			print(self.RED+'Exception in contact timing extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in contact timing  extraction  -->'+str(e)+'\n')
			self.contact_dict['timing'] = 'Not Found'

		try:
			self.contact_dict['vendor'] = self.driver.find_element(By.XPATH, '//*[@id="officeHoursSection"]//*[@class="vendorName"]').text
		except Exception as e:
			print(self.RED+'Exception in contact vendor extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  contace vendor  extraction  -->'+str(e)+'\n')
			self.contact_dict['vendor'] = 'Not Found'




	def get_policies(self):
		self.policy_dict={}
		try:
			self.policy_dict['dog_policy'] = self.driver.find_element(By.XPATH, '//*[@id="profileV2FeesWrapper"]/div[1]//*[@class="component-body"]').text
		except Exception as e:
			print(self.RED+'Exception in dog policy extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in dog policies  extraction  -->'+str(e)+'\n')
			self.policy_dict['dog_policy'] = 'Not Found'

		try:
			self.policy_dict['cat_policy'] = self.driver.find_element(By.XPATH, '//*[@id="profileV2FeesWrapper"]/div[2]//*[@class="component-body"]').text
		except Exception as e:
			print(self.RED+'Exception in contact cat policy extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  cat policies  extraction  -->'+str(e)+'\n')
			self.policy_dict['cat_policy'] = 'Not Found'


	def get_collage(self):
		collage_list =[]
		try:
			elements_data = self.driver.find_elements(By.XPATH, '//*[@id="profilev2College"]//tbody//tr')
		except Exception as e:
			print(self.RED+'Exception in education main XPATH extraction system exiting !! -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  education main XPATH system exiting  extraction !! -->'+str(e)+'\n')
			exit('system exiting in education main XPATH extracting')
		for i in elements_data:
			collage_dict={}
			try:
				collage_dict['collage_name']=i.find_element(By.XPATH, '//td[1]//*[@class="transportationName"]/a').text
			except Exception as e:
				print(self.RED+'Exception in collage name extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  collage name  extraction  -->'+str(e)+'\n')
				collage_dict['collage_name']='Not Found'

			try:
				collage_dict['collage_drive']=i.find_element(By.XPATH, './td[2]').text
			except Exception as e:
				print(self.RED+'Exception in collage drive extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  collage derivee  extraction  -->'+str(e)+'\n')
				collage_dict['collage_drive'] = 'Not Found'

			try:
				collage_dict['collage_distance']=i.find_element(By.XPATH, './td[3]').text
			except Exception as e:
				print(self.RED+'Exception in collage distance extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  collage distance  extraction  -->'+str(e)+'\n')
				collage_dict['collage_distance']= 'Not Found'
			collage_list.append(collage_dict)
		return collage_list



	def get_school(self):
		school_list=[]
		try:
			elements_data = self.driver.find_elements(By.XPATH, '//*[@id="profilev2SchoolsModule"]//*[@class="card"]')
		except Exception as e:
			print(self.RED+'Exception in main public schools main XPATH extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  public schools main XPATH  extraction  -->'+str(e)+'\n')
			exit('system exiting in school main XPATH details')
		for i in elements_data:
			school_dict={}
			try:
				school_dict['school_name']=i.find_element(By.XPATH, '//*[@class="title"]/a').get_attribute('title')
			except Exception as e:
				print(self.RED+'Exception in school name extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  school name  extraction  -->'+str(e)+'\n')
				school_dict['school_name']= 'Not Found'

			try:
				school_dict['school_type']=i.find_element(By.XPATH, '//*[@class="header-column"]//div[2]').text
				if school_dict['school_type'] =='':
					self.driver.find_element(By.XPATH, '//*[@id="privateTabText"]').click()
					school_dict['school_type']=i.find_element(By.XPATH, '//*[@class="header-column"]//div[2]').text
			except Exception as e:
				print(self.RED+'Exception in school type extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  school type  extraction  -->'+str(e)+'\n')
				school_dict['school_type']= 'Not Found'

			try:
				school_dict['school_details']=i.find_element(By.XPATH, '//*[@class="body-column"]/div[1]').text
			except Exception as e:
				print(self.RED+'Exception in school details extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  school details  extraction  -->'+str(e)+'\n')
				school_dict['school_details']= 'Not Found'

			try:
				school_dict['school_rating']=i.find_element(By.XPATH, '//*[@class="body-column-right"]/div[1]').text
			except Exception as e:
				print(self.RED+'Exception in school rating extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  school rating  extraction  -->'+str(e)+'\n')
				school_dict['school_rating']= 'Not Found'
			school_list.append(school_dict)
		return school_list



	def get_eduction(self):
		self.eduction_list = []
		education_dict={}
		education_dict['collage'] = self.get_collage()
		education_dict['school'] = self.get_school()
		self.eduction_list.append(education_dict)


	def get_sound_score(self):
		try:    
			html = self.driver.find_element(By.TAG_NAME, 'html')
			height_counter =0
			soundscore=''
			while True:
				num=self.driver.find_element(By.XPATH, "//*[@id='soundScoreSection']/div[1]/div[1]/div[2]").text
				print(num)
				#num = score.text
				if num.isdigit():
					soundscore = self.driver.find_element(By.XPATH, "//*[@id='soundScoreSection']/div[1]").text
					break
				html.send_keys(Keys.PAGE_DOWN)
				time.sleep(2)

				if height_counter > 50:
					break
				height_counter=height_counter+1
			return soundscore
		except:
			traceback.print_exc()
			return soundscore



	def scroll_page(self):
		SCROLL_PAUSE_TIME = 0.5
		last_height = self.driver.execute_script("return document.body.scrollHeight")
		while True:
			self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
			time.sleep(SCROLL_PAUSE_TIME)
			new_height = self.driver.execute_script("return document.body.scrollHeight")
			if new_height == last_height:
				break
			last_height = new_height
		sh=self.driver.find_elements(By.XPATH, '//*[@id="anchors"]//*[@class="slick-track"]/button')
		for i in sh:
			i.click()
			time.sleep(1)



	def get_review(self):
		data_elements = self.driver.find_elements(By.XPATH, '//*[@id="reviewsWrapper"]//*[@class="reviewcontainerwrapper"]')
		for ele in data_elements:
			review_dict = {}
			try:
				review_dict['single_rating']  = ele.find_element(By.XPATH, '//*[@class="reviewRatingDaysSincePostedContainer"]/span[1]').get_attribute('content')
			except Exception as e:
				review_dict['single_rating'] = 'Not Found'
				print(self.RED+'Exception in getting single_rating extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  get single rating  extraction  -->'+str(e)+'\n')

			try:
				review_dict['single_review_date']  = ele.find_element(By.XPATH, '//*[@class="reviewDateContainer"]').get_attribute('title')
			except Exception as e:
				review_dict['single_review_date'] = 'Not Found'
				print(self.RED+'Exception in getting single_review_date extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  single review data  extraction  -->'+str(e)+'\n')

			try:
				review_dict['single_review_comment']  = ele.find_element(By.XPATH, '//*[@class="reviewTextContainer"]').text
			except Exception as e:
				review_dict['single_review_comment'] = 'Not Found'
				print(self.RED+'Exception in getting single_review_comment extraction -->'+str(e)+self.RESET)
				self.log_file.write('Exception in  review comment  extraction  -->'+str(e)+'\n')

			self.review_list.append(review_dict)

	

	
	def get_review_rating(self):
		self.review_rating_list=[]
		review_rating_dict={}
		try:
			review_rating_dict['rating'] = self.driver.find_element(By.XPATH, '//*[@id="reviewsWrapper"]//*[@class="averageRating"]').text
		except Exception as e:
			review_rating_dict['rating'] = 'Not Found'
			print(self.RED+'Exception in getting review rating extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  getting review rating  extraction  -->'+str(e)+'\n')

		try:
			review_rating_dict['total_review'] = self.driver.find_element(By.XPATH, '//*[@id="reviewsWrapper"]//*[@class="renterReviewsLabel"]').text
		except Exception as e:
			review_rating_dict['total_review'] = 'Not Found'
			print(self.RED+'Exception in getting total review extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  total review  extraction  -->'+str(e)+'\n')
		
		try:
			sub_name = self.driver.find_element(By.XPATH, '//*[@id="reviewsWrapper"]//*[@class="paginationWrapper"]//*[@class="storyicon right2StoryIcon"]')
			main_name = sub_name.find_element(By.XPATH, "..")
			self.review_list =[]
			while main_name.get_attribute('class') !='disabled':
				self.get_review()
				sub_name.click()
				time.sleep(1)
			self.get_review()
			review_rating_dict['reviews'] = self.review_list
		except Exception as e:
			review_rating_dict['reviews'] = []
			print(self.RED+'Exception in getting pagination class extraction -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  pagination page  extraction  -->'+str(e)+'\n')

		self.review_rating_list.append(review_rating_dict)


	def is_JSON_exist(self):
		if (os.path.exists(self.url_list_path)):
			return True
		else:
			return False


	def start_scrape(self):
		self.read_city_fromCSV()
		print(self.GREEN+'Total cities will process :-'+str(len(self.all_city_name)))
		self.log_file.write('Total cities will process :-'+str(len(self.all_city_name))+'\n')
		city_count=1
		for city in self.all_city_name:
			self.mydict['city_name']=city
			self.url_list_path = self.urls_file_path+self.mydict['city_name']+'_list'+".json"
			dt = "{:%B %d, %Y %H-%M-%S}".format(datetime.now())
			print("*"*40+'  '+str(city_count)+". PROCESS START "+self.mydict['city_name']+" ("+str(dt)+") "+"*"*40)
			self.log_file.write("*"*40+'  '+str(city_count)+". PROCESS START "+self.mydict['city_name']+" ("+str(dt)+") "+"*"*40+'\n')
			if self.is_JSON_exist():
				#exit(self.url_list_path)
				with open(self.url_list_path) as f:
					urls_list_data = json.load(f)
					self.alldataUrls = urls_list_data['list']
					print(self.GREEN+'Successfully load '+self.mydict['city_name']+' city data'+self.RESET)
					self.log_file.write('Successfully load '+self.mydict['city_name']+' city data'+'\n')
			else:
				#exit('iminelse stattment')
				print(self.RED+'Json url file does not exists'+self.RESET+'\n'+self.WARNING+'please wait while we are extracting '+self.mydict['city_name']+' city data....'+self.RESET)
				self.log_file.write('Json url file does not exists'+'\n'+'please wait while we are extracting '+self.mydict['city_name']+' city data....'+'\n')
				self.getURL()
				self.listingURL()

			print(self.WARNING+'Total property --> '+str(len(self.alldataUrls))+self.RESET+'\n'+'-'*30+' start property process'+'-'*30)
			self.log_file.write('Total property --> '+str(len(self.alldataUrls))+'\n'+'-'*30+' start property process'+'-'*30+'\n')
			#self.alldataUrls = ['https://www.apartments.com/santa-clara-square-apartment-homes-santa-clara-ca/rqgtnx3/',https://www.apartments.com/708-3rd-ave-redwood-city-ca/47y842k/','https://www.apartments.com/1111-morse-st-san-jose-ca/nm6cs7m/','https://www.apartments.com/olive-tree-apartments-sunnyvale-ca/pycgvvt/','https://www.apartments.com/civic-plaza-apartments-santa-clara-ca/cwpvpfe/']
			#self.alldataUrls = ['https://www.apartments.com/the-flats-at-cityline-sunnyvale-ca/0cdemn2/','https://www.apartments.com/anton-ladera-mountain-view-ca/fjs69kv/','https://www.apartments.com/santa-clara-square-apartment-homes-santa-clara-ca/rqgtnx3/','https://www.apartments.com/modera-broadway-seattle-wa/s4djne4/',]
			#self.alldataUrls = ['https://www.apartments.com/modera-broadway-seattle-wa/s4djne4/','https://www.apartments.com/avalon-silicon-valley-sunnyvale-ca/we0jcgj/']
			#self.alldataUrls = ['https://www.apartments.com/prado-santa-clara-ca/b8re69b/']
			#self.alldataUrls = ['https://www.apartments.com/the-flats-at-cityline-sunnyvale-ca/0cdemn2/','https://www.apartments.com/santa-clara-square-apartment-homes-santa-clara-ca/rqgtnx3/','https://www.apartments.com/modera-broadway-seattle-wa/s4djne4/']
			for url in self.alldataUrls:
				try:
					print(self.WARNING+str(self.count)+'. '+self.RESET+'Processing property --> '+url)
					self.log_file.write(str(self.count)+'. Processing property --> '+url+'\n')
					self.driver.get(url)
					time.sleep(5)
					self.scroll_page()
					self.get_appartment_data(url,city)    #return the self.apartment_dict which contain apartment data 
					print(self.WARNING+'** Apartment Basic Details extracted **'+self.RESET)
					self.get_floorwise_plan()      # this function will collect the all details of floorwise unit. 
					print(self.WARNING+'** Floorwise Plan Details extracted **'+self.RESET)
					self.get_amenities()          # this functio will collect the aminities details
					print(self.WARNING+'** Amenities extracted **'+self.RESET)
					self.get_contact_details()     # getting contact details.
					print(self.WARNING+'** FContact Details extracted **'+self.RESET)
					self.get_policies()
					print(self.WARNING+'** Ploicies Details extracted **'+self.RESET)
					self.get_eduction()
					print(self.WARNING+'** Education Details extracted **'+self.RESET)
					self.soundscore = self.get_sound_score()
					self.get_review_rating()
					print(self.WARNING+'** Reviews Details extracted **'+self.RESET)
					self.insert_property_data()
					#exit()
					#current_data_id = self.get_appartment_data(url)
					# self.get_services_data(current_data_id)
					# self.get_property_detail(url,current_data_id)
					# self.count = self.count+1
				
				except Exception as e:
					print(self.RED+'Exception -->'+str(e)+self.RESET)
					self.count= self.count+1
					continue
				print('-'*40+' property processed'+'-'*40)
				# if self.count >=6:
				#    break
			
			#self.exportCSV()
			self.count=1
			city_count=city_count+1
			dt = "{:%B %d, %Y %H-%M-%S}".format(datetime.now())
			print("*"*40+"PROCESS END FOR "+self.mydict['city_name']+" ("+str(dt)+") "+"*"*40)
		self.driver.close()
		dt = "{:%B %d, %Y %H-%M-%S}".format(datetime.now())
		print("="*50+"PROCESS END ("+str(dt)+") "+"="*50)



	def is_exist_record(self):
		check_query ="select * from appartment where property_url like '%s' and property_name= '%s'"%('%'+self.appartment_dict['propertyurl']+'%',self.appartment_dict['propertyName'])
		try:
			self.cursor.execute(check_query)
			row_count = self.cursor.rowcount
			if row_count == 0:
				return False
			else:
				updated = self.cursor.fetchone()    
				self.updated_id = updated['id']
				self.is_updated = True
				return True
		except Exception as e:
			print ('Exception in MYSQL Query (** is_exist_record **) #--->',str(e))
			self.log_file.write('Exception in  Is record exist !! -->'+str(e)+'\n')



	def insert_property_data(self):
		try:
			if self.is_exist_record():
				#sql = "UPDATE appartment SET property_address='%s', city='%s', rent_special ='%s', bedroom ='%s', bathroom ='%s', area ='%s', move_in_special ='%s', floor_plan ='%s', amenities ='%s', contact ='%s', polices ='%s', education ='%s', transportation ='%s', review ='%s', property_modified =CURRENT_TIMESTAMP WHERE id = %s"%(str(self.appartment_dict['addressEle']),self.appartment_dict['propertycity'],str(self.appartment_dict['rent_special']),self.appartment_dict['bedroom'],self.appartment_dict['bathroom'],self.appartment_dict['area'],self.appartment_dict['move_in_special'],json.dumps(self.floorwise_datalist),json.dumps(self.aminities_dict),json.dumps(self.contact_dict),json.dumps(self.policy_dict),json.dumps(self.eduction_list),str(self.soundscore),json.dumps(self.review_rating_list),int(self.updated_id))                
				sql = ("UPDATE appartment SET property_address=%s, city=%s, rent_special =%s, bedroom =%s, bathroom =%s, area =%s, move_in_special =%s, floor_plan =%s, amenities =%s, contact =%s, polices =%s, education =%s, transportation =%s, review =%s, property_modified =CURRENT_TIMESTAMP WHERE id = %s")
				data = (str(self.appartment_dict['addressEle']),self.appartment_dict['propertycity'],str(self.appartment_dict['rent_special']),self.appartment_dict['bedroom'],self.appartment_dict['bathroom'],self.appartment_dict['area'],self.appartment_dict['move_in_special'],json.dumps(self.floorwise_datalist),json.dumps(self.aminities_dict),json.dumps(self.contact_dict),json.dumps(self.policy_dict),json.dumps(self.eduction_list),str(self.soundscore),json.dumps(self.review_rating_list),int(self.updated_id))
				print('Updating ...... !!')
			else:
				#sql = "INSERT INTO appartment (property_name, property_url,property_address,city,rent_special,bedroom,bathroom,area,move_in_special,floor_plan,amenities,contact,polices,education,transportation,review,property_created,property_modified) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"%(str(self.appartment_dict['propertyName']),str(self.appartment_dict['propertyurl']),self.appartment_dict['addressEle'],self.appartment_dict['propertycity'],self.appartment_dict['rent_special'],self.appartment_dict['bedroom'],self.appartment_dict['bathroom'],self.appartment_dict['area'],self.appartment_dict['move_in_special'],json.dumps(self.floorwise_datalist),json.dumps(self.aminities_dict),json.dumps(self.contact_dict),json.dumps(self.policy_dict),json.dumps(self.eduction_list),str(self.soundscore),json.dumps(self.review_rating_list))
				sql = ("INSERT INTO appartment (property_name, property_url,property_address,city,rent_special,bedroom,bathroom,area,move_in_special,floor_plan,amenities,contact,polices,education,transportation,review,property_created,property_modified)"
				"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
				data = (str(self.appartment_dict['propertyName']),str(self.appartment_dict['propertyurl']),self.appartment_dict['addressEle'],self.appartment_dict['propertycity'],self.appartment_dict['rent_special'],self.appartment_dict['bedroom'],self.appartment_dict['bathroom'],self.appartment_dict['area'],self.appartment_dict['move_in_special'],json.dumps(self.floorwise_datalist),json.dumps(self.aminities_dict),json.dumps(self.contact_dict),json.dumps(self.policy_dict),json.dumps(self.eduction_list),str(self.soundscore),json.dumps(self.review_rating_list))
				print('Inserting ....... !!')
			#print(sql)
			self.cursor.execute(sql,data)
			self.db.commit()
			print('Property Success !!')
		except Exception as e:
			print(self.RED+'Exception in insert_property_data insertion -->'+str(e)+self.RESET)
			self.log_file.write('Exception in  insert/update propert  extraction  -->'+str(e)+'\n')
			#traceback.print_exc()


	def get_services_fromdb(self,id):
		service_list = []
		#print('##',id)
		query = "SELECT p.policy_name,s.services_type,s.service_value FROM property_policies as p INNER JOIN property_services as s on s.services_type = p.id where appartment_id="+str(id)
		self.cursor.execute(query)
		#query_data = self.cursor.fetchall()
		query_data = [item for item in self.cursor.fetchall()]
		for data in query_data:
			service_dict={}
			if data['services_type'] ==2 and data['policy_name']=='Pet policy':
				service_dict['PET'] = data['service_value']
				#print('#---PET---#',data['service_value'])
			# else:
			#   print('NO data found for PET!!!!!')
			#----------------------------------------------------
			if data['services_type'] ==1 and data['policy_name']=='Lease length':
				service_dict['LEASE'] = data['service_value']  
				#print('#---LEASE---#',data['service_value'])
			# else:
			#   print('NO data Found for Lease!!!!!')
			#-----------------------------------------------------
			if data['services_type'] ==3 and data['policy_name']=='Services':
				service_dict['OTHER']= data['service_value']
				#print('#---OTHER SERVICES---#',data['service_value'])
			# else:
			#   print('NO data for other services!!!!!!')
			#-----------------------------------------------------
			if data['services_type'] ==4 and data['policy_name']=='Sound score':
				service_dict['SOUND'] = data['service_value']
				#print('#---SOUNDSCORE---#',query_data[3]['service_value'])
			# else:
			#   print('NO data for SOUND SCORE!!!!!!')
			#-----------------------------------------------------
			if data['services_type'] ==5 and data['policy_name']=='Walk score':
				service_dict['WALK'] = data['service_value']
				#print('#---WALKSCORE---#',query_data[4]['service_value'])
			# else:
			#   print('NO data for Walk score!!!!!!')
			#-----------------------------------------------------
			if data['services_type'] ==6 and data['policy_name']=='School data':
				service_dict['SCHOOL'] = data['service_value']
				#print('#---SCHOOLDATA---#',query_data[5]['service_value'])
			# else:
			#   print('NO data for School data!!!!!!')
			#-----------------------------------------------------
			service_list.append(service_dict)
		#print('****************************************************************************',service_list)
		return service_list
		
	def get_all_data_fromdb(self):
		set_bit = True
		query = "SELECT * FROM propery_details pd LEFT JOIN  appartment a on pd.appartment_id = a.id where city like '%s'"%(self.mydict['city_name'])
		#print(query)
		#exit()
		self.cursor.execute(query)
		excel_list=[]
		#if self.cursor.rowcount():
		query_data = self.cursor.fetchall()
		#   print(query_data)
		for data in query_data:
			self.excel_dict={}
				#if set_bit:
			services_data = self.get_services_fromdb(data['appartment_id'])
			#print('##############',services_data)  
			for i in services_data:
				if 'PET' in i.keys():
					self.excel_dict['Pet Policy'] = i
					#print('##------PET HAI   HAI',self.excel_dict['pet_policy'])
				# else:
				#   self.excel_dict['pet_policy'] = 'NO PET POLICY DATA'

				if 'LEASE' in i.keys():
					self.excel_dict['Lease Policy'] = i
					#print('##------LEASE HAI',self.excel_dict['lease_policy'])
				# else:
				#   self.excel_dict['lease_policy'] = 'NO LEASE POLICY DATA'

				if 'OTHER' in i.keys():
					self.excel_dict['Other Services'] = i
					#print('##------OTHER HAI',self.excel_dict['other_services'])
				# else:
				#   self.excel_dict['other_services'] = 'NO OTHER SERVICES DATA'
				if 'SOUND' in i.keys():
					self.excel_dict['Sound Score'] = i
					#print('##------SOUND BHI HAI ',self.excel_dict['sound_score'])
				# else:
				#   self.excel_dict['sound_score'] = 'NO SOUND SCORE DATA'
				if 'WALK' in i.keys():
					self.excel_dict['Walk Score'] = i
					#print('##------WALK BHI HAI!!!',self.excel_dict['walk_score'])
				# else:
				#   self.excel_dict['walk_score'] = 'NO WALK SCORE DATA'
				if 'SCHOOL' in i.keys():
					self.excel_dict['School Data'] = i
					#print('##------SCHOOL HAI!!!!!',self.excel_dict['school_data'])
				# else:
				#   self.excel_dict['school_data'] = 'NO SCHOOL DATA'
				#set_bit = False
			self.excel_dict['ID'] = data['appartment_id']
			self.excel_dict['Name'] = data['property_name']
			self.excel_dict['Address'] = data['property_address']
			self.excel_dict['City'] = data['city']
			self.excel_dict['Rent Special'] = data['rent_special']
			self.excel_dict['Property Type'] = data['propery_type']
			self.excel_dict['Price'] = data['price']
			self.excel_dict['Area'] = data['area']
			self.excel_dict['Availability'] = data['availability']
			self.excel_dict['URL'] = data['property_url']
			self.excel_dict['Property Created'] = data['property_created'].strftime('%Y-%m-%d %H-%M-%S') 
			excel_list.append(self.excel_dict)
			#print('EXCELL------',excel_list)
			#exit()
		return excel_list

	def turncatetables(self):
		try:
			self.cursor.execute("TRUNCATE TABLE appartment")
			self.cursor.execute("TRUNCATE TABLE property_services")
			self.cursor.execute("TRUNCATE TABLE propery_details")
			print('Successfully turncated !!!')

		except Exception as e:
			print(self.RED+'Exception  -->>'+str(e)+self.RESET)

	def modified_csv(self, filename):
		dt = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
		with open(filename) as fin:
			with open(self.csv_output+str(self.mydict['city_name'])+'_'+dt+'.csv', 'w', newline='') as fout:
				reader = csv.DictReader(fin, delimiter='|')
				writer = csv.DictWriter(fout, reader.fieldnames, delimiter='|')
				writer.writeheader()
				writer.writerows(reader)

	def exportCSV(self):
		self.all_list=[]
		csv_columns = ['ID','Name','Address','City','Rent Special','Property Type','Price','Area','Availability','Pet Policy','Lease Policy','Other Services','Sound Score','Walk Score','School Data','URL','Property Created']
		dt = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
		csv_name='C:/WebScrap_Batch_Automation/city_exported_csvdata/'+str(self.mydict['city_name'])+'_'+dt+'.csv'
		with open(csv_name, 'w') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='|')
			writer.writeheader()
			self.all_list = self.get_all_data_fromdb()
			for data in self.all_list:
				writer.writerow(data)
		self.modified_csv(csv_name)
		self.turncatetables()

	def get_expoted_data(self):
		self.all_data=[]
		query = "select * from appartment"
		self.cursor.execute(query)
		#query_data = self.cursor.fetchall()
		#['id', 'property_name', 'property_url', 'property_address', 'city', 'rent_special', 'bedroom', 'bathroom', 'area', 'move_in_special', 'floor_plan', 'amenities', 'contact', 'polices', 'education', 'transportation', 'review', 'property_created', 'property_modified'])
		[self.all_data.append(x)  for x in self.cursor.fetchall()]
		#['id', 'property_name', 'property_url', 'property_address', 'city', 'rent_special', 'bedroom', 'bathroom', 'area', 'move_in_special', 'floor_plan', 'amenities', 'contact', 'polices', 'education', 'transportation', 'review', 'property_created', 'property_modified']
		
		#csv_column_common= ['id', 'property_name', 'property_url', 'property_address', 'city', 'rent_special', 'bedroom', 'bathroom', 'area', 'move_in_special', 'property_created', 'property_modified']
		#self.make_csv_single('Apartment_details',csv_column_common)

		#csv_column_floor_plan= ['id', 'property_name', 'floor_plan', 'property_created', 'property_modified']
		#self.make_csv_floorwise_plan('Floorwise_plan_details',csv_column_floor_plan)


		csv_column = ['id', 'property_name', 'property_url', 'property_address', 'city', 'rent_special', 'bedroom', 'bathroom', 'area', 'move_in_special', 'amenities', 'contact', 'polices', 'education', 'transportation', 'review', 'property_created', 'property_modified', 'floor', 'price_range', 'apt_type', 'total_unit', 'unit_no', 'unitprice', 'unitarea', 'unitavailable']
		self.makecsv2()



	def makecsv1(self,filename,csv_column):
		ts = datetime.now().strftime("%Y%m%d%I%M%S")
		csv_name = self.csv_output+filename+'_'+str(ts)+'.csv'
		with open(csv_name, 'w') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csv_column, delimiter='|')
			writer.writeheader()
			for d in self.all_data:
				print(d)
				exit()
				for k,v in d.copy().items():
					if k=='floor_plan':
						data = json.loads(d['floor_plan'])
						for i in data:
							d.update(i)
							for x in i['unit']:
								d.update(x)
				del d['floor_plan']
				del d['unit']
				writer.writerow(d)

	def makecsv2(self):
		extended_attr = ['amenities', 'contact', 'polices']
		result = []
		for property in self.all_data:
			for floor in json.loads(property["floor_plan"]):
				for unit in floor["unit"]:
					tmp = {**property, **floor, **unit}
					del tmp["floor_plan"]
					del tmp["unit"]
					for attr in extended_attr:
						tmp.update(json.loads(property[attr],strict=False))
						del tmp[attr]
					result.append(tmp)

		keys = result[0].keys()
		ts = datetime.now().strftime("%Y%m%d%I%M%S")
		csv_name = self.csv_output+'apartments'+'_'+str(ts)+'.csv'
		with open(csv_name, 'w', newline='')  as output_file:
			dict_writer = csv.DictWriter(output_file, keys, delimiter='|')
			dict_writer.writeheader()
			dict_writer.writerows(result)
		


	def make_csv_single(self,filename,csv_column):
		ts = datetime.now().strftime("%Y%m%d%I%M%S")
		csv_name = self.csv_output+'filename'+'_'+str(ts)+'.csv'
		with open(csv_name, 'w') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csv_column,delimiter='|')
			writer.writeheader()
			for data in self.all_data:
				data = self.key_exist(data,csv_column)
				writer.writerow(data)

	def make_csv_floorwise_plan(self,filename,csv_column):
		ts = datetime.now().strftime("%Y%m%d%I%M")
		csv_name = self.csv_output+filename+'_'+str(ts)+'.csv'
		with open(csv_name, 'w') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csv_column,delimiter='|')
			writer.writeheader()
			for data in self.all_data:
				print(data)
				exit()
				#data = json.loads(data['floor_plan'])
				data = self.key_exist(data,csv_column)
				writer.writerow(data)

				data = json.loads(data['floor_plan'])
				for i in data:
					print(i)



	def key_exist(self,key_dict,key_list):
		for key, value in key_dict.copy().items():
			if key not in key_list:
				key_dict.pop(key)
		return key_dict




	   # self.driver.close()
obj = Scrape()
obj.start_scrape()
obj.get_expoted_data()

#obj.exportCSV()
#obj.turncatetables()
