package com.example.accessingdatacassandra;

import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@RequestMapping("/api/vets")
public class VetController {
    private final VetRepository vetRepository;
    public VetController(VetRepository vetRepository) {
        this.vetRepository = vetRepository;
    }
    @GetMapping
    public List<Vet> getAllVets() {
        return StreamSupport.stream(vetRepository.findAll().spliterator(), false).collect(Collectors.toUnmodifiableList());
    }

    @GetMapping("/{name}")
    public Vet getVetByFirstName(@PathVariable String name) {
        return vetRepository.findByFirstName(name);
    }

    @PostMapping
    public Vet createVet(@RequestBody Vet vet) {
        return vetRepository.save(vet);
    }
}
