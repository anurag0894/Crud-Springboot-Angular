package Service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import DAO.Employee_DAO;
import Model.Job_Status;
import Model.User_details;

@Service
@Transactional
public class Employee_Service_Imp implements Employee_Service{
 
	@Autowired
	private Employee_DAO studentdao;
	
	@Override
	public boolean saveStudent(User_details student) {
		return studentdao.saveStudent(student);
	}

	@Override
	public List<Job_Status> getStatus() {
		return studentdao.getStatus();
	}
	
	@Override
	public List<User_details> getStudents() {
		return studentdao.getStudents();
	}

	@Override
	public boolean deleteStudent(User_details student) {
		return studentdao.deleteStudent(student);
	}

	@Override
	public List<User_details> getStudentByID(User_details student) {
		return studentdao.getStudentByID(student);
	}

	@Override
	public boolean updateStudent(User_details student) {
		return studentdao.updateStudent(student);
	}

	

}
