package Model;


import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="job_status")
public class Job_Status {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private int id;
	private String Job_Id;
	private String Job_Description;
	private String Job_Status;
	private String Contact_Person_Details;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getJob_Id() {
		return Job_Id;
	}
	public void setJob_Id(String job_Id) {
		Job_Id = job_Id;
	}
	public String getJob_Description() {
		return Job_Description;
	}
	public void setJob_Description(String job_Description) {
		Job_Description = job_Description;
	}
	public String getJob_Status() {
		return Job_Status;
	}
	public void setJob_Status(String job_Status) {
		Job_Status = job_Status;
	}
	public String getContact_Person_Details() {
		return Contact_Person_Details;
	}
	public void setContact_Person_Details(String contact_Person_Details) {
		Contact_Person_Details = contact_Person_Details;
	}
	
	
	

}
