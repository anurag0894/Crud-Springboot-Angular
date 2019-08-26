package Model;


import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;



@Entity
@Table(name="user_details")
public class User_details {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private int employee_id;
	
	private String employee_title;
	private String employee_username;
	private String employee_gender;	
	private String employee_dateOfBirth;
	private String employee_address;	
	private String employee_state;	
	private String employee_country;	
	private String employee_maritalStatus;
	private String employee_qualification;
	private String employee_exp;
	private String employee_skills;
	private String employee_certification;
	private String employee_pan;
	private String employee_mobile_number;
	private String employee_language;
	private String employee_email;


	public int getEmployee_id() {
		return employee_id;
	}
	public void setEmployee_id(int employee_id) {
		this.employee_id = employee_id;
	}
	public String getEmployee_title() {
		return employee_title;
	}
	public void setEmployee_title(String employee_title) {
		this.employee_title = employee_title;
	}
	public String getEmployee_username() {
		return employee_username;
	}
	public void setEmployee_username(String employee_username) {
		this.employee_username = employee_username;
	}
	public String getEmployee_gender() {
		return employee_gender;
	}
	public void setEmployee_gender(String employee_gender) {
		this.employee_gender = employee_gender;
	}
	public String getEmployee_dateOfBirth() {
		return employee_dateOfBirth;
	}
	public void setEmployee_dateOfBirth(String employee_dateOfBirth) {
		this.employee_dateOfBirth = employee_dateOfBirth;
	}
	public String getEmployee_address() {
		return employee_address;
	}
	public void setEmployee_address(String employee_address) {
		this.employee_address = employee_address;
	}
	public String getEmployee_state() {
		return employee_state;
	}
	public void setEmployee_state(String employee_state) {
		this.employee_state = employee_state;
	}
	public String getEmployee_country() {
		return employee_country;
	}
	public void setEmployee_country(String employee_country) {
		this.employee_country = employee_country;
	}
	public String getEmployee_maritalStatus() {
		return employee_maritalStatus;
	}
	public void setEmployee_maritalStatus(String employee_maritalStatus) {
		this.employee_maritalStatus = employee_maritalStatus;
	}
	public String getEmployee_qualification() {
		return employee_qualification;
	}
	public void setEmployee_qualification(String employee_qualification) {
		this.employee_qualification = employee_qualification;
	}
	public String getEmployee_exp() {
		return employee_exp;
	}
	public void setEmployee_exp(String employee_exp) {
		this.employee_exp = employee_exp;
	}
	public String getEmployee_skills() {
		return employee_skills;
	}
	public void setEmployee_skills(String employee_skills) {
		this.employee_skills = employee_skills;
	}
	public String getEmployee_certification() {
		return employee_certification;
	}
	public void setEmployee_certification(String employee_certification) {
		this.employee_certification = employee_certification;
	}
	public String getEmployee_pan() {
		return employee_pan;
	}
	public void setEmployee_pan(String employee_pan) {
		this.employee_pan = employee_pan;
	}
	public String getEmployee_mobile_number() {
		return employee_mobile_number;
	}
	public void setEmployee_mobile_number(String employee_mobile_number) {
		this.employee_mobile_number = employee_mobile_number;
	}
	public String getEmployee_language() {
		return employee_language;
	}
	public void setEmployee_language(String employee_language) {
		this.employee_language = employee_language;
	}
	public String getEmployee_email() {
		return employee_email;
	}
	public void setEmployee_email(String employee_email) {
		this.employee_email = employee_email;
	}
	

	
}
