using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;

public class EtlProcessor
{
    private readonly DbContext dbContext1;
    private readonly DbContext dbContext2;
    private readonly DbContext dbContext3;

    public EtlProcessor()
    {
        dbContext1 = new DbContext1();
        dbContext2 = new DbContext2();
        //dbContext3 = new DbContext3();
    }

    public async Task RemoveDuplicatesAsync()
    {
        var dataFromEntity1 = await dbContext1.Set<Entity1>().ToListAsync();
        var dataFromEntity2 = await dbContext2.Set<Entity2>().ToListAsync();
        //var dataFromEntity3 = await dbContext3.Set<Entity3>().ToListAsync();


        var distinctDataFromEntity1 = dataFromEntity1
            .GroupBy(item => item.Id)
            .Select(group => group.First())
            .ToList();

        var distinctDataFromEntity2 = dataFromEntity2
            .GroupBy(item => item.Id)
            .Select(group => group.First())
            .ToList();

        //var distinctDataFromEntity3 = dataFromEntity3
        //    .GroupBy(item => item.Id)
        //    .Select(group => group.First())
        //    .ToList();

        dbContext1.Set<Entity1>().RemoveRange(dataFromEntity1.Except(distinctDataFromEntity1));
        dbContext2.Set<Entity2>().RemoveRange(dataFromEntity2.Except(distinctDataFromEntity2));
        //dbContext3.Set<Entity3>().RemoveRange(dataFromEntity3.Except(distinctDataFromEntity3));

        await dbContext1.SaveChangesAsync();
        await dbContext2.SaveChangesAsync();
        //await dbContext3.SaveChangesAsync();
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        var etlProcessor = new EtlProcessor();
        await etlProcessor.RemoveDuplicatesAsync();

        Console.WriteLine("Duplicates removing process complete.");
    }
}
