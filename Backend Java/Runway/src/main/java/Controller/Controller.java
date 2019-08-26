package Controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import Model.User_details;
import Model.Job_Status;
import Service.Employee_Service;

@RestController
@CrossOrigin(origins="http://localhost:4200")
@RequestMapping(value="/api")
public class Controller {

	@Autowired
	private Employee_Service studentservice;
	
	@PostMapping("save-student")
	public boolean saveStudent(@RequestBody User_details student) {
		 return studentservice.saveStudent(student);
		
	}
	
	@GetMapping("students-list")
	public List<User_details> allstudents() {
		 return studentservice.getStudents();
	}
	
	@GetMapping("job-list")
	public List<Job_Status> allstatus() {
		 return studentservice.getStatus();
	}
	
	@DeleteMapping("delete-student/{student_id}")
	public boolean deleteStudent(@PathVariable("student_id") String student_id,User_details student) {
		student.setEmployee_email(student_id);
		return studentservice.deleteStudent(student);
	}

	@GetMapping("student/{student_id}")
	public List<User_details> allstudentByID(@PathVariable("student_id") String student_id,User_details student) {
		 student.setEmployee_email(student_id);
		 return studentservice.getStudentByID(student);
		
	}
	
	@PostMapping("update-student/{student_id}")
	public boolean updateStudent(@RequestBody User_details student,@PathVariable("student_id") String student_id) {
		student.setEmployee_email(student_id);
		return studentservice.updateStudent(student);
	}
}