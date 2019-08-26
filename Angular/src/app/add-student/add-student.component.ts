import { Component, OnInit } from '@angular/core';
import { StudentService } from '../student.service';
import {FormControl,FormGroup,Validators} from '@angular/forms';
import { Student } from '../student';
@Component({
  selector: 'app-add-student',
  templateUrl: './add-student.component.html',
  styleUrls: ['./add-student.component.css']
})
export class AddStudentComponent implements OnInit {

  constructor(private studentservice:StudentService) { }

  student : Student=new Student();
  submitted = false;

  ngOnInit() {
    this.submitted=false;
  }

  studentsaveform=new FormGroup({
    Title:new FormControl(),
    Username:new FormControl(),
    Gender:new FormControl(),
    DateOfBirth:new FormControl(),
    Address:new FormControl(),
    State:new FormControl(),
    Country:new FormControl(),
    maritalStatus:new FormControl(),
    qualification:new FormControl(),
    exp:new FormControl(),
    skills:new FormControl(),
    certificate:new FormControl(),
    pan:new FormControl(),
    mobile_number:new FormControl(),
    language:new FormControl(),
    email:new FormControl()
  });

  saveStudent(saveStudent){
    this.student=new Student();   
    this.student.employee_title=this.employee_title.value;
    this.student.employee_username=this.employee_username.value;
    this.student.employee_gender=this.employee_gender.value;
    this.student.employee_dateOfBirth=this.employee_dateOfBirth.value;
    this.student.employee_address=this.employee_address.value;
    this.student.employee_state=this.employee_state.value;
    this.student.employee_country=this.employee_country.value;
    this.student.employee_maritalStatus=this.employee_maritalStatus.value;
    this.student.employee_qualification=this.employee_qualification.value;
    this.student.employee_exp=this.employee_exp.value;
    this.student.employee_skills=this.employee_skills.value;
    this.student.employee_certification=this.employee_certification.value;
    this.student.employee_pan=this.employee_pan.value;
    this.student.employee_language=this.employee_language.value;
    this.student.employee_email=this.employee_email.value;
    this.student.employee_mobile_number=this.employee_mobile_number.value;

    this.submitted = true;
    this.save();

  }
  save() {
    console.log(this.student.employee_title);
    this.studentservice.createStudent(this.student)
      .subscribe(data => console.log(data), error => console.log(error));
    this.student = new Student();
  }

  get employee_title(){
    return this.studentsaveform.get('Title');
  }

  get employee_username(){
    return this.studentsaveform.get('Username');
  }

  get employee_gender(){
    return this.studentsaveform.get('Gender');
  }

  get employee_address(){
    return this.studentsaveform.get('Address');
  }

  get employee_state(){
    return this.studentsaveform.get('State');
  }
  get employee_country(){
    return this.studentsaveform.get('Country');
  }
  get employee_maritalStatus(){
    return this.studentsaveform.get('maritalStatus');
  }
  get employee_qualification(){
    return this.studentsaveform.get('qualification');
  }
  get employee_exp(){
    return this.studentsaveform.get('exp');
  }
  get employee_dateOfBirth(){
    return this.studentsaveform.get('DateOfBirth');
  }

  get employee_certification(){
    return this.studentsaveform.get('certificate');
  }
  get employee_pan(){
    return this.studentsaveform.get('pan');
  }
  get employee_language(){
    return this.studentsaveform.get('language');
  }
  get employee_email(){
    return this.studentsaveform.get('email');
  }

  get employee_skills(){
    return this.studentsaveform.get('skills');
  }

  get employee_mobile_number(){
    return this.studentsaveform.get('mobile_number');
  }


  addStudentForm(){
    this.submitted=false;
    this.studentsaveform.reset();
  }
}
