package study.querydslstudy.entity;

import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@SpringBootTest
@Transactional
//@Commit 다음 테스트할때 깨질수도 있기 때문에 주석처리
class MemberTest {

    @Autowired
    EntityManager em;

    @Test
    public void testEntity(){
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member m1 = new Member("m1", 15, teamA);
        Member m2 = new Member("m2", 21, teamA);

        Member m3 = new Member("m3", 52, teamB);
        Member m4 = new Member("m4", 62, teamB);

        em.persist(m1);
        em.persist(m2);
        em.persist(m3);
        em.persist(m4);

        //초기화
        em.flush();
        em.clear();

        List<Member> members = em.createQuery("select m from Member m", Member.class)
                .getResultList();
        for (Member member : members) {
            System.out.println("m" + member);
            System.out.println(" -> m.team " + member.getTeam());
        } // 실무에서는 sout 하지말고 Assertion으로 검증하는 방식으로 테스트 해야한다.
    }
    

}