import { Component, OnInit } from '@angular/core';
import { StudentService } from '../student.service';
import { Student } from '../student';
import { Observable,Subject } from "rxjs";

import {FormControl,FormGroup,Validators} from '@angular/forms';

@Component({
  selector: 'app-student-list',
  templateUrl: './student-list.component.html',
  styleUrls: ['./student-list.component.css']
})
export class StudentListComponent implements OnInit {

 constructor(private studentservice:StudentService) { }

  studentsArray: any[] = [];
  dtOptions: DataTables.Settings = {};
  dtTrigger: Subject<any>= new Subject();


  students: Observable<Student[]>;
  student : Student=new Student();
  deleteMessage=false;
  studentlist:any;
  isupdated = false;    
 

  ngOnInit() {
    this.isupdated=false;
    this.dtOptions = {
      pageLength: 6,
      stateSave:true,
      lengthMenu:[[6, 16, 20, -1], [6, 16, 20, "All"]],
      processing: true
    };   
    this.studentservice.getStudentList().subscribe(data =>{
    this.students =data;
    this.dtTrigger.next();
    })
  }
  
  deleteStudent(id: number) {
    this.studentservice.deleteStudent(id)
      .subscribe(
        data => {
          console.log(data);
          this.deleteMessage=true;
          this.studentservice.getStudentList().subscribe(data =>{
            this.students =data
            })
        },
        error => console.log(error));
  }


  updateStudent(id: number){
    this.studentservice.getStudent(id)
      .subscribe(
        data => {
          this.studentlist=data           
        },
        error => console.log(error));
  }

  studentupdateform=new FormGroup({
    employee_id:new FormControl(),
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

  updateStu(updstu){
    this.student=new Student(); 
    this.student.employee_id=this.employee_id.value;
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
   console.log(this.employee_mobile_number.value);
   

   this.studentservice.updateStudent(this.employee_email.value,this.student).subscribe(
    data => {     
      this.isupdated=true;
      this.studentservice.getStudentList().subscribe(data =>{
        this.students =data
        })
    },
    error => console.log(error));
  }

  get employee_id(){
    return this.studentupdateform.get('employee_id');
    
  }

  get employee_title(){
    return this.studentupdateform.get('Title');
  }

  get employee_username(){
    return this.studentupdateform.get('Username');
  }

  get employee_gender(){
    return this.studentupdateform.get('Gender');
  }

  get employee_address(){
    return this.studentupdateform.get('Address');
  }

  get employee_state(){
    return this.studentupdateform.get('State');
  }
  get employee_country(){
    return this.studentupdateform.get('Country');
  }
  get employee_maritalStatus(){
    return this.studentupdateform.get('maritalStatus');
  }
  get employee_qualification(){
    return this.studentupdateform.get('qualification');
  }
  get employee_exp(){
    return this.studentupdateform.get('exp');
  }
  get employee_dateOfBirth(){
    return this.studentupdateform.get('DateOfBirth');
  }

  get employee_certification(){
    return this.studentupdateform.get('certificate');
  }
  get employee_pan(){
    return this.studentupdateform.get('pan');
  }
  get employee_language(){
    return this.studentupdateform.get('language');
  }
  get employee_email(){
    return this.studentupdateform.get('email');
  }

  get employee_skills(){
    return this.studentupdateform.get('skills');
  }

  get employee_mobile_number(){
    return this.studentupdateform.get('mobile_number');
  }

  changeisUpdate(){
    this.isupdated=false;
  }
}
