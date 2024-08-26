import psycopg2
from faker import Faker
import random

fake = Faker()

db_config = {
    "dbname": "demodb",
    "user": "demouser",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to the database successfully!")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

def generate_employee_data():
    return {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "city": fake.city(),
        "state": fake.state(),
        "district": fake.city(),
        "pincode": fake.zipcode(),
        "departments": fake.random_element(elements=("HR", "Engineering", "Marketing", "Sales", "Finance")),
        "phone_number": fake.msisdn()[:10],
        "salary": round(random.uniform(30000, 150000), 2),
        "email_id": fake.unique.email(),
        "age": random.randint(22, 60),
        "total_years_of_experience": random.randint(1, 40),
        "father_name": fake.name(),
        "mother_name": fake.name_female(),
        "previous_company": fake.company(),
        "date_of_birth": fake.date_of_birth(minimum_age=22, maximum_age=60),
        "gender": fake.random_element(elements=("Male", "Female", "Other")),
        "marital_status": fake.random_element(elements=("Single", "Married", "Divorced", "Widowed")),
        "hire_date": fake.date_between(start_date="-10y", end_date="today"),
        "job_title": fake.job(),
        "job_level": fake.random_element(elements=("Junior", "Mid", "Senior", "Lead", "Manager")),
        "nationality": fake.country(),
        "emergency_contact_name": fake.name(),
        "emergency_contact_phone": fake.msisdn()[:10],
        "employee_type": fake.random_element(elements=("Full-Time", "Part-Time", "Contractor", "Intern")),
        "office_location": fake.city(),
        "manager_name": fake.name(),
        "medical_conditions": fake.random_element(elements=(None, "Diabetes", "Hypertension", "None")),
        "blood_group": fake.random_element(elements=("A+", "B+", "O+", "AB+", "A-", "B-", "O-", "AB-")),
        "languages_known": ', '.join(fake.words(nb=random.randint(1, 4),
                                                ext_word_list=['English', 'Spanish', 'French', 'German', 'Mandarin',
                                                               'Hindi'])),
        "skills": ', '.join(fake.words(nb=random.randint(2, 5),
                                       ext_word_list=['Python', 'Java', 'SQL', 'Project Management', 'Communication',
                                                      'Leadership'])),
        "educational_qualifications": fake.random_element(elements=("Bachelor's", "Master's", "PhD", "Diploma")),
        "certifications": ', '.join(fake.words(nb=random.randint(0, 3),
                                               ext_word_list=['PMP', 'AWS Certified', 'Scrum Master', 'Data Science'])),
        "joining_bonus": round(random.uniform(5000, 20000), 2),
        "current_projects": ', '.join(
            fake.words(nb=random.randint(1, 3), ext_word_list=['Project Alpha', 'Beta', 'Gamma', 'Delta'])),
        "performance_rating": fake.random_element(elements=("Excellent", "Good", "Average", "Below Average")),
    }

def insert_employee_data_batch(employee_batch):
    query = """
        INSERT INTO employee_information (
            first_name, last_name, city, state, district, pincode, departments, 
            phone_number, salary, email_id, age, total_years_of_experience, father_name, 
            mother_name, previous_company, date_of_birth, gender, marital_status, 
            hire_date, job_title, job_level, nationality, emergency_contact_name, 
            emergency_contact_phone, employee_type, office_location, manager_name, 
            medical_conditions, blood_group, languages_known, skills, educational_qualifications, 
            certifications, joining_bonus, current_projects, performance_rating
        ) VALUES (
            %(first_name)s, %(last_name)s, %(city)s, %(state)s, %(district)s, %(pincode)s, %(departments)s, 
            %(phone_number)s, %(salary)s, %(email_id)s, %(age)s, %(total_years_of_experience)s, %(father_name)s, 
            %(mother_name)s, %(previous_company)s, %(date_of_birth)s, %(gender)s, %(marital_status)s, 
            %(hire_date)s, %(job_title)s, %(job_level)s, %(nationality)s, %(emergency_contact_name)s, 
            %(emergency_contact_phone)s, %(employee_type)s, %(office_location)s, %(manager_name)s, 
            %(medical_conditions)s, %(blood_group)s, %(languages_known)s, %(skills)s, %(educational_qualifications)s, 
            %(certifications)s, %(joining_bonus)s, %(current_projects)s, %(performance_rating)s
        );
        """
    try:
        cursor.executemany(query, employee_batch)
        conn.commit()
        print(f"Batch of {len(employee_batch)} records inserted successfully!")
    except Exception as e:
        print(f"Error inserting data batch: {e}")
        conn.rollback()

batch_size = 10000
employee_batch = []
for _ in range(3500000):
    employee_data = generate_employee_data()
    employee_batch.append(employee_data)

    if len(employee_batch) == batch_size:
        insert_employee_data_batch(employee_batch)
        employee_batch.clear()

if employee_batch:
    insert_employee_data_batch(employee_batch)

cursor.close()
conn.close()
