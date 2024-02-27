package study.querydslstudy.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString(of = {"id", "username", "age"}) // 주의! 여기에 team 넣으면 무한참조 일어남
public class Member {

    @Id
    @GeneratedValue
    @Column(name = "member_id")
    private Long id;
    private String username;
    private int age;

    @ManyToOne(fetch = FetchType.LAZY) // **One은 LAZY로 받아야 한다.
    @JoinColumn(name = "item_id") // 컬럼명
    private Team team;

    public Member(String username, int age){
        this(username, age, null);
    }

    public Member(String username){
        this(username, 0);
    }

    public Member(String username, int age, Team team) {
        this.username = username;
        this.age = age;
        if (team != null) {
            changeTeam(team);
        }
    }

    public void changeTeam(Team team) {
        this.team = team;
        team.getMembers().add(this);
    }
}
