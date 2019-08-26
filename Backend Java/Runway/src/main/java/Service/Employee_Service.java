package Service;

import java.util.List;

import Model.*;

public interface Employee_Service {

	
	public boolean saveStudent(User_details student);
	public List<User_details> getStudents();
	public boolean deleteStudent(User_details student);
	public List<User_details> getStudentByID(User_details student);
	public boolean updateStudent(User_details student);
	public List<Job_Status> getStatus();
}
