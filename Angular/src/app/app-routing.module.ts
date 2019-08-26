import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { RegisterComponent } from './register/register.component';
import { PartnerlistComponent } from './partnerlist/partnerlist.component';
import { StudentListComponent } from './student-list/student-list.component';
import { AddStudentComponent } from './add-student/add-student.component';


const routes: Routes = [
  { path: '', redirectTo: 'view-employee', pathMatch: 'full' },
  { path: 'view-employee', component: StudentListComponent },
  { path: 'add-employee', component: AddStudentComponent },
  { path: 'workflow' , component: PartnerlistComponent},
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
