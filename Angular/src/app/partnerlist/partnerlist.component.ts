import { Component, OnInit } from '@angular/core';
import { StudentService } from '../student.service';
import { Partner } from '../partner';
import { Observable,Subject } from "rxjs";

import {FormControl,FormGroup,Validators} from '@angular/forms';

@Component({
  selector: 'app-partner-list',
  templateUrl: './partnerlist.component.html',
  styleUrls: ['./partnerlist.component.css']
})
export class PartnerlistComponent implements OnInit {

 constructor(private studentservice:StudentService) { }

  partnersArray: any[] = [];
  dtOptions: DataTables.Settings = {};
  dtTrigger: Subject<any>= new Subject();


  partners: Observable<Partner[]>;
  partner : Partner=new Partner();
  deleteMessage=false;
  partnerlist:any;
  isupdated = false;    
 

  ngOnInit() {
    this.isupdated=false;
    this.dtOptions = {
      pageLength: 6,
      stateSave:true,
      lengthMenu:[[6, 16, 20, -1], [6, 16, 20, "All"]],
      processing: true
    };   
    this.studentservice.getPartnersList().subscribe(data =>{
    this.partners =data;
    console.log(this.partners);
    this.dtTrigger.next();
    }
    )


  }

  downloadFile(){
    let link = document.createElement("a");
    link.download = "Bond_Document";
    link.href = "assets/Bond_Document.JPG";
    link.click();
}

  
  /*
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
  downloadFile(){
    let link = document.createElement("a");
    link.download = "Bond_Document";
    link.href = "assets/Bond_Document.JPG";
    link.click();
}

  */

}
