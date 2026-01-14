package ru.develonica.kosovanov.controller;

import lombok.RequiredArgsConstructor;
import ru.develonica.kosovanov.actor.RootActor;
import ru.develonica.kosovanov.service.UserActionSendService;
import ru.develonica.kosovanov.model.UserAction;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequiredArgsConstructor
public class UserActionController {

    private final UserActionSendService sendService;
    private final RootActor rootActor;

    @PostMapping("/userActionList")
    public void userAction(@RequestBody List<UserAction> actions) throws Exception {
        CompletableFuture<?>[] futures = actions.stream()
                .map(sendService::sendUserAction)
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).get();
    }

    @GetMapping("/actionStats")
    public Map<String, Long> getActionStats() {
        return rootActor.queryState();
    }

}
