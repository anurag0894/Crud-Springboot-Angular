package DAO ;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import Model.Job_Status;
import Model.User_details;

@Repository
public class Employee_DAO_Imp  implements Employee_DAO{

	@Autowired
	private SessionFactory sessionFactory;
	
	@Override
	public boolean saveStudent(User_details student) {
		boolean status=false;
		try {
			sessionFactory.getCurrentSession().save(student);
			status=true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	@Override
	public List<User_details> getStudents() {
		Session currentSession = sessionFactory.getCurrentSession();
		Query<User_details> query=currentSession.createQuery("from User_details", User_details.class);
		List<User_details> list=query.getResultList();
		return list;
	}
	
	@Override
	public List<Job_Status> getStatus() {
		Session currentSession = sessionFactory.getCurrentSession();
		Query<Job_Status> query=currentSession.createQuery("from Job_Status", Job_Status.class);
		List<Job_Status> list=query.getResultList();
		return list;
	} 

	@Override
	public boolean deleteStudent(User_details student) {
		boolean status=false;
		try {
			sessionFactory.getCurrentSession().delete(student);
			status=true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	@Override
	public List<User_details> getStudentByID(User_details student) {
		Session currentSession = sessionFactory.getCurrentSession();
		Query<User_details> query=currentSession.createQuery("from User_details where employee_email=:student_id", User_details.class);
		query.setParameter("student_id", student.getEmployee_email());
		List<User_details> list=query.getResultList();
		return list;
	}

	@Override
	public boolean updateStudent(User_details student) {
		boolean status=false;
		try {
			sessionFactory.getCurrentSession().update(student);
			status=true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	

}
