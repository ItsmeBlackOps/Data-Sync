import google.generativeai as palm
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
import pymysql
import json

app = Flask(__name__)
CORS(app)

palm.configure(
    api_key="AIzaSyDLQehxg9kSK5MqCI1N0GiDrcT9Re-XV-c"
)

generation_config = {
  "temperature": 0,
  "top_p": 1,
  "top_k": 1,
  "max_output_tokens": 2048,
}



timeout = 10

db_config = {
    "charset": "utf8mb4",
    "connect_timeout": timeout,
    "cursorclass": pymysql.cursors.DictCursor,
    "db": "defaultdb",
    "host": "mysql-286ab86e-harshp-c41f.aivencloud.com",
    "password": "AVNS_s3Ckp3dyXV0bg4aXK0A",
    "read_timeout": timeout,
    "port": 13002,
    "user": "avnadmin",
    "write_timeout": timeout,
}

def insert_candidate_data(candidate_data):
    try:
        # Connect to the database
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Check if the candidate already exists
        cursor.execute("SELECT Candidate_ID FROM Candidates WHERE Candidate_Name = %s", (candidate_data["Candidate_Name"],))
        existing_candidate = cursor.fetchone()

        if existing_candidate:
            print(f"Duplicate candidate found with Candidate_ID: {existing_candidate['Candidate_ID']}")
            return existing_candidate['Candidate_ID']
        else:
            # Insert candidate data into Candidates table
            insert_candidate_query = """
            INSERT INTO Candidates (Candidate_Name, Birth_Date, Gender, Education, University,
            Total_Experience, State, Technology, Email_ID, Contact_Number)
            VALUES (%(Candidate_Name)s, %(Birth_Date)s, %(Gender)s, %(Education)s, %(University)s,
            %(Total_Experience)s, %(State)s, %(Technology)s, %(Email_ID)s, %(Contact_Number)s)
            """
            cursor.execute(insert_candidate_query, candidate_data)

            # Commit the changes to the database
            connection.commit()

            # Retrieve the auto-generated Candidate_ID
            cursor.execute("SELECT LAST_INSERT_ID()")
            candidate_id = cursor.fetchone()['LAST_INSERT_ID()']

            print(f"Candidate data inserted successfully with Candidate_ID: {candidate_id}")
            return candidate_id

    finally:
        connection.close()


def insert_task_data(candidate_id, task_data):
    try:
        # Connect to the database
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        check_duplicate_query = """
        SELECT Task_ID FROM Task2 
        WHERE Support_Subject = %(Support_Subject)s
        """
        cursor.execute(check_duplicate_query, task_data)
        duplicate_task = cursor.fetchone()

        if duplicate_task:
            print("Duplicate task found. Skipping insertion.")
            return
        
        # Insert task data into Tasks table
        task_data["Candidate_ID"] = candidate_id
        insert_task_query = """
        INSERT INTO Task2 (Candidate_ID, Task_Type, Support_Subject, End_Client, Job_Title, Duration, Interview_Round, Interview_Datetime)
        VALUES (%(Candidate_ID)s, %(Task_Type)s, %(Support_Subject)s, %(End_Client)s, %(Job_Title)s, %(Duration)s, %(Interview_Round)s, %(Interview_Datetime)s)
        """
        cursor.execute(insert_task_query, task_data)
        
        # Get the auto-generated Task_ID
        task_id = cursor.lastrowid

        # Insert Task_ID into Task_Completion_Details table
        insert_completion_query = """
        INSERT INTO Task_Completion_Details (Task_ID, Completion_Status)
        VALUES (%s, %s)

        """
        completion_status = 'Pending'  # Set your default completion status here
        # Adjust the completion date based on your requirements or set it to NULL if it's not available at this stage
        completion_date = None
        cursor.execute(insert_completion_query, (task_id, completion_status))

        # Commit the changes to the database
        connection.commit()

        print("Task data inserted successfully.")

    except pymysql.Error as e:
        print(f"An error occurred: {e}")

    finally:
        connection.close()



@app.route('/process_data', methods=['POST'])
def process_data():
    if request.method == 'POST':
        data = request.data.decode('utf-8')
        prompt = f"""
Fill Up Following Table From Given Text and give exact details do basic data validation if possible must use the same format just fill up placeholders Remove The Square Brackates, Not A Single Extra Field Is Required
        Candidate Name: [Name]
        Birth date: [Birth Date]
        Gender: [Gender]
        Education: [Education Level] 
        University: [University Name]
        Total Experience in Years:[Convert Years of Experience in to Integer Data Type, Make sure it is INTEGER VALUE AND WHOLE INTEGER]  # Convert the total years of experience into a whole integer value, ensuring it's positive and represents only numbers without decimals or any other characters.
        State:[State] 
        Technology: [Technology]
        End Client: [End Client]
        Interview Round: [Interview Round]
        Job Title in JD: [Job Title]
        Email ID: [Email]
        Personal Contact Number: [Contact Number]
        Date and Time of Interview (Mention time zone): [Convert Date and Time of Interview start time to "YYYY-MON-DD HH:MM:SS AM/PM TimeZone(EST/CST/MST/PST)" format.]
        Duration: [Convert Duration into minutes] # Validate for integer value, duration in minutes
        Subject: [Subject Title of Email]
        Task Type: [Based on Subject It Should Be Either Mock Interview / Interview Support / Assessment Support / Screening Interview]
        {data}
        """
    model = palm.GenerativeModel(model_name="gemini-pro", generation_config=generation_config)
    completion = palm.generate_text(
        prompt=prompt,
        temperature=0,
        # The maximum length of response
        max_output_tokens=800,
    )

    text = completion.result
    name_pattern = r"Candidate Name: (.+)"
    birth_date_pattern = r"Birth date: (.+)"
    gender_pattern = r"Gender: (.+)"
    education_pattern = r"Education: (.+)"
    university_pattern = r"University: (.+)"
    experience_pattern = r"Total Experience in Years: (\d+)"
    state_pattern = r"State: (.+)"
    technology_pattern = r"Technology: (.+)"
    end_client_pattern = r"End Client: (.+)"
    round_pattern = r"Interview Round: (.+)"
    job_title_pattern = r"Job Title in JD: (.+)"
    email_pattern = r"Email ID: (.+)"
    contact_number_pattern = r"Personal Contact Number: (.+)"
    interview_datetime_pattern = r"Date and Time of Interview \(Mention time zone\): (.+)"
    duration_pattern = r"Duration:\s*(\d+)"
    Subject_pattern = r"Subject: (.+)"
    Task_type_pattern = r"Task Type: (.+)"

    candidate_name = re.search(name_pattern, text).group(1)
    birth_date = re.search(birth_date_pattern, text).group(1)
    gender = re.search(gender_pattern, text).group(1)
    education = re.search(education_pattern, text).group(1)
    university = re.search(university_pattern, text).group(1)
    experience = re.search(experience_pattern, text).group(1)
    state = re.search(state_pattern, text).group(1)
    technology = re.search(technology_pattern, text).group(1)
    end_client = re.search(end_client_pattern, text).group(1)
    round = re.search(round_pattern, text).group(1) if round_pattern else None
    job_title = re.search(job_title_pattern, text).group(1)
    email = re.search(email_pattern, text).group(1)
    contact_number = re.search(contact_number_pattern, text).group(1)
    interview_datetime = re.search(interview_datetime_pattern, text).group(1) if interview_datetime_pattern else None
    duration = re.search(duration_pattern, text).group(1)
    Subject = re.search(Subject_pattern, text).group(1)
    Task_type = re.search(Task_type_pattern, text).group(1)

    prompt = f"""
  Instructions:

Identify the provided time in the specific US time zone (EST, CST, PST, MST).

Convert the provided time to New York time (Eastern Time Zone/EST) by considering the following conversions:

From Central Time Zone (CST) to Eastern Time Zone (EST): Add 1 hour to the provided time.
From Pacific Time Zone (PST) to Eastern Time Zone (EST): Add 3 hours to the provided time.
From Mountain Time Zone (MST) to Eastern Time Zone (EST): Add 2 hours to the provided time.
Output the validated time in New York in the required datetime format ("%Y-%m-%d %H:%M:%S").

Validation Steps:

If the provided time is already in Eastern Standard Time (EST), no conversion is needed. The output remains the same as the input.
Examples:

If the provided time is in Central Time Zone (CST) and you want to convert it to New York time:

Original time in CST: 01/03/2024, 14:30:00
Add 1 hour to convert to EST: 01/03/2024, 15:30:00 (New York Time)
Convert to the specified datetime format: 2024-01-03 15:30:00
If the provided time is already in Eastern Standard Time (EST):

Original time in EST: 2024-01-08 05:00:00
No conversion needed.
Output remains the same: 2024-01-08 05:00:00
If the provided time is in Pacific Time Zone (PST) and you want to convert it to New York time:

Original time in PST: 01/03/2024, 12:00:00
Add 3 hours to convert to EST: 01/03/2024, 15:00:00 (New York Time)
Convert to the specified datetime format: 2024-01-03 15:00:00



    {interview_datetime}
    """

    filter_data = palm.generate_text(
        prompt=prompt,
        temperature=0,
        # The maximum length of response
        max_output_tokens=800,
    )
    interview_datetime = filter_data.result
    candidate_data = {
    "Candidate_Name": candidate_name,
    "Birth_Date": birth_date,
    "Gender": gender,
    "Education": education,
    "University": university,
    "Total_Experience": int(experience),
    "State": state,
    "Technology": technology,
    
    "Email_ID": email,
    "Contact_Number": contact_number,
}
    
    candidate_id = insert_candidate_data(candidate_data)

    task_data = {
    "Task_Type": Task_type,
    "Support_Subject": Subject,
    "End_Client": end_client,
    "Job_Title": job_title,
    "Duration": duration,
    "Interview_Round": round,
    "Interview_Datetime": interview_datetime
}
    
    insert_task_data(candidate_id, task_data)

    return json.dumps(data, default=str), 200

@app.route('/tasks', methods=['GET'])
def get_tasks():
    try:
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            sql = """

SELECT 
    Task2.Task_ID,
    Task2.Candidate_ID,
    Candidates.Candidate_Name,
    Task2.Task_Type,
    Task2.Support_Subject,
    Task2.End_Client,
    Task2.Job_Title,
    Task2.Duration,
    Task2.Interview_Round,
    Task2.Interview_Datetime,
    td.Completion_Status,
    td.Feedback,
    td.Task_Completed_By
FROM 
    Task2 
JOIN 
    Task_Completion_Details td 
ON 
    Task2.Task_ID = td.Task_ID
JOIN 
    Candidates 
ON 
    Task2.Candidate_ID = Candidates.Candidate_ID
WHERE 
    CAST(Task2.Interview_Datetime AS DATE) = DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)
ORDER BY
    Task2.Interview_Datetime;


"""

            cursor.execute(sql)
            tasks = cursor.fetchall()
            return json.dumps(tasks, default=str), 200

    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        connection.close()

@app.route('/tasks/update', methods=['POST'])
def update_task():
    try:
        data = request.get_json()

        task_id = data.get('Task_ID')
        completion_status = data.get('Completion_Status')
        feedback = data.get('Feedback')
        task_completed_by = data.get('Task_Completed_By')

        # Connect to the database
        connection = pymysql.connect(**db_config)

        with connection.cursor() as cursor:
            update_query = """
                UPDATE Task_Completion_Details 
                SET Completion_Status = %s, Feedback = %s, Task_Completed_By = %s
                WHERE Task_ID = %s
                """

            # Execute the update query
            cursor.execute(update_query, (completion_status, feedback, task_completed_by, task_id))

            # Commit the changes to the database
            connection.commit()

            print("Task updated successfully.")  # This should print if the update was successful

        return jsonify({'message': 'Task updated successfully'}), 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True)