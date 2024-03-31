package com.arjunsk.nested;

import java.util.ArrayList;
import java.util.List;

public class NestedJoin {
    public static void main(String[] args) {
        // Sample data for Employees
        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(1, "Alice", 101));
        employees.add(new Employee(2, "Bob", 102));
        employees.add(new Employee(3, "Charlie", 103));
        employees.add(new Employee(4, "David", 101));

        // Sample data for Departments
        List<Department> departments = new ArrayList<>();
        departments.add(new Department(101, "HR"));
        departments.add(new Department(102, "Tech"));
        departments.add(new Department(103, "Finance"));

        // Perform Nested Join
        List<String> joinedData = nestedJoin(employees, departments);

        // Print the joined data
        joinedData.forEach(System.out::println);
    }

    public static List<String> nestedJoin(List<Employee> employees, List<Department> departments) {
        List<String> results = new ArrayList<>();
        for (Employee employee : employees) {
            for (Department department : departments) {
                if (employee.departmentId == department.id) {
                    results.add(employee.name + " works in " + department.name);
                    break; // Assuming department IDs are unique, we can break once a match is found
                }
            }
        }
        return results;
    }
}
