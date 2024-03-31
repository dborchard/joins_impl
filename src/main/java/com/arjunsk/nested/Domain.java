package com.arjunsk.nested;

class Employee {
    int id;
    String name;
    int departmentId;

    public Employee(int id, String name, int departmentId) {
        this.id = id;
        this.name = name;
        this.departmentId = departmentId;
    }
}

class Department {
    int id;
    String name;

    public Department(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
